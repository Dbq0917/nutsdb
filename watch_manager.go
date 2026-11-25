package nutsdb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/nutsdb/nutsdb/internal/utils"
)

// errors
var (
	ErrBucketSubscriberNotFound = errors.New("bucket subscriber not found")
	ErrKeySubscriberNotFound    = errors.New("key subscriber not found")
	ErrSubscriberNotFound       = errors.New("subscriber not found")
	ErrWatchChanCannotSend      = errors.New("watch channel cannot send")
	ErrKeyAlreadySubscribed     = errors.New("key already subscribed")
	ErrWatchManagerClosed       = errors.New("watch manager closed")
	ErrWatchingCallbackFailed   = errors.New("watching callback failed")
	ErrWatchingChannelClosed    = errors.New("watching channel closed")
	ErrChannelNotAvailable      = errors.New("channel not available")
	ErrCloseWatchManagerTimeout = errors.New("close watch manager timeout")
)

// convert these variables to var for testing
var (
	watchChanBufferSize      = 1024
	receiveChanBufferSize    = 1024
	maxBatchSize             = 1024
	deadMessageThreshold     = 100
	distributeChanBufferSize = 128
	victimBucketBufferSize   = 128
	// slowSubscriberRetryInterval is how long the distributor waits between
	// delivery attempts when a subscriber channel is full.
	slowSubscriberRetryInterval = 5 * time.Millisecond
)

const (
	DefaultCallbackTimeout = 1 * time.Second
)

// message priority
const (
	MessagePriorityHigh   = iota // the messages must be ensured to deliver to distributor
	MessagePriorityMedium = 1    // the messages may be dropped
)

type (
	bucketToSubscribers       map[core.BucketName]map[string]map[uint64]*subscriber
	victimBucketToSubscribers map[uint64]map[string]map[uint64]*subscriber
	victimBucketChan          chan victimBucketToSubscribers
	MessagePriority           int

	WatchOptions struct {
		CallbackTimeout time.Duration
	}

	MessageOptions struct {
		Priority MessagePriority
	}
)

type Message struct {
	BucketName core.BucketName
	Key        string
	Value      []byte
	Flag       DataFlag
	Timestamp  uint64
	// Seq is a globally monotonic sequence number assigned when the message
	// enters the watch manager. It lets subscribers detect gaps after the
	// backpressure safety valve evicts a slow consumer.
	Seq      uint64
	priority MessagePriority
}

// WatchMetrics exposes counters describing how the watch pipeline applies
// backpressure. Every counter is safe to read concurrently.
type WatchMetrics struct {
	// AcceptedMessages counts messages successfully enqueued into the pipeline.
	AcceptedMessages int64
	// DeliveredMessages counts messages pushed into a subscriber channel.
	DeliveredMessages int64
	// RejectedMessages counts messages refused at ingress because the watch
	// channel was full; the corresponding write receives an error instead of
	// a silent success.
	RejectedMessages int64
	// DroppedBatches and DroppedMessages count batches (and their messages)
	// discarded by the collector while the manager is shutting down.
	DroppedBatches  int64
	DroppedMessages int64
	// EvictedSubscribers counts slow subscribers force-unsubscribed after
	// their channels stayed full across deadMessageThreshold attempts.
	EvictedSubscribers int64
	// EvictedMessages counts messages discarded when a subscriber was evicted.
	EvictedMessages int64
}

func NewMessage(bucketName core.BucketName, key string, value []byte, flag DataFlag, timestamp uint64, options ...MessageOptions) *Message {
	var priority MessagePriority
	// default priority is medium
	priority = MessagePriorityMedium

	if len(options) > 0 {
		priority = options[0].Priority
	}

	return &Message{
		BucketName: bucketName,
		Key:        key,
		Value:      value,
		Flag:       flag,
		Timestamp:  timestamp,
		priority:   priority,
	}
}

func NewWatchOptions() *WatchOptions {
	return &WatchOptions{
		CallbackTimeout: DefaultCallbackTimeout,
	}
}

// WithCallbackTimeout sets the callback timeout
func (opts *WatchOptions) WithCallbackTimeout(timeout time.Duration) {
	opts.CallbackTimeout = timeout
}

type subscriber struct {
	id           uint64
	bucketName   core.BucketName
	key          string
	receiveChan  chan *Message
	deadMessages int
	active       atomic.Bool
}

type watchManager struct {
	lookup         bucketToSubscribers // bucketName -> key -> id -> subscriber
	watchChan      chan *Message       // the hub channel to receive the messages
	distributeChan chan []*Message     // the collector worker collects messages from watchChan and sends them to the distributor worker through distributeChan
	victimMaps     victimBucketToSubscribers
	victimChan     victimBucketChan
	workerCtx      context.Context // cancellation for in-flight tasks
	workerCancel   context.CancelFunc
	wg             sync.WaitGroup

	// sequence generator for messages accepted by the pipeline
	seqGenerator atomic.Uint64

	// backpressure counters, see WatchMetrics for their meaning
	acceptedMessages   atomic.Int64
	deliveredMessages  atomic.Int64
	rejectedMessages   atomic.Int64
	droppedBatches     atomic.Int64
	droppedMessages    atomic.Int64
	evictedSubscribers atomic.Int64
	evictedMessages    atomic.Int64

	closed      bool
	started     bool // indicates whether Start() has been called
	idGenerator *IDGenerator

	muClosed  sync.RWMutex
	muStarted sync.RWMutex
	mu        sync.Mutex
}

func NewWatchManager() *watchManager {
	ctx := context.Background()
	workerCtx, workerCancel := context.WithCancel(ctx)
	return &watchManager{
		lookup:         make(bucketToSubscribers),
		watchChan:      make(chan *Message, watchChanBufferSize),
		distributeChan: make(chan []*Message, distributeChanBufferSize),
		closed:         false,
		started:        false,
		workerCtx:      workerCtx,
		workerCancel:   workerCancel,
		idGenerator:    &IDGenerator{currentMaxId: 0},
		victimMaps:     make(victimBucketToSubscribers),
		victimChan:     make(victimBucketChan, victimBucketBufferSize),
	}
}

// Metrics returns a point-in-time snapshot of the watch pipeline counters.
// It can be called at any time, including after the manager is closed.
func (wm *watchManager) Metrics() WatchMetrics {
	return WatchMetrics{
		AcceptedMessages:   wm.acceptedMessages.Load(),
		DeliveredMessages:  wm.deliveredMessages.Load(),
		RejectedMessages:   wm.rejectedMessages.Load(),
		DroppedBatches:     wm.droppedBatches.Load(),
		DroppedMessages:    wm.droppedMessages.Load(),
		EvictedSubscribers: wm.evictedSubscribers.Load(),
		EvictedMessages:    wm.evictedMessages.Load(),
	}
}

// Name returns the component name
func (wm *watchManager) Name() string {
	return "WatchManager"
}

// send a message to the watch manager
func (wm *watchManager) sendMessage(message *Message) error {
	if wm.isClosed() {
		return ErrWatchManagerClosed
	}

	// assign the sequence before enqueuing so a worker that picks the message
	// up immediately never observes an unset Seq (avoids a data race)
	message.Seq = wm.seqGenerator.Add(1)

	// the high priority messages must be ensured to push to the watch channel
	if message.priority == MessagePriorityHigh {
		select {
		case wm.watchChan <- message:
			wm.acceptedMessages.Add(1)
			utils.GetLogger().Printf("[watch_manager] Sent high priority message %s/%s to watch channel\n", message.BucketName, message.Key)
			return nil
		case <-wm.workerCtx.Done():
			return ErrWatchManagerClosed
		}
	}

	select {
	case wm.watchChan <- message:
		wm.acceptedMessages.Add(1)
		return nil
	case <-wm.workerCtx.Done():
		return ErrWatchManagerClosed
	default:
		wm.rejectedMessages.Add(1)
		return ErrWatchChanCannotSend
	}
}

func (wm *watchManager) sendUpdatedEntries(entries []*core.Entry, deletedbuckets map[core.BucketName]bool, getBucketName func(bucketId core.BucketId) (core.BucketName, error)) error {
	if wm.isClosed() {
		return ErrWatchManagerClosed
	}

	// send all updated entries to the watch manager
	if len(entries) > 0 {
		for _, entry := range entries {
			bucketName, err := getBucketName(entry.Meta.BucketId)
			if err != nil {
				continue
			}

			rawKey, err := entry.GetRawKey()
			if err != nil {
				utils.GetLogger().Printf("get raw key %+v error: %+v", entry.Key, err)
				continue
			}

			message := NewMessage(bucketName, string(rawKey), entry.Value, entry.Meta.Flag, entry.Meta.Timestamp)
			if err := wm.sendMessage(message); err != nil {
				return err
			}
		}
	}

	//
	for bucketName := range deletedbuckets {
		message := NewMessage(bucketName, "", nil, DataBucketDeleteFlag, uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityHigh})
		if err := wm.sendMessage(message); err != nil {
			return err
		}
	}

	return nil
}

// startDistributor starts both the collector and distributor goroutines
func (wm *watchManager) startDistributor() {
	defer wm.cleanUpSubscribers()

	// start the victim collector goroutine
	// it collects the victim buckets from the victim channel
	// and handle delete bucket operation
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runVictimCollector()
	}()

	// Start the distributor goroutine (consumes from distributeChan)
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runDistributor()
	}()

	// start the collector goroutine (collects messages into batches)
	wm.wg.Add(1)
	go func() {
		defer wm.wg.Done()
		wm.runCollector()
	}()

	wm.wg.Wait()
}

// runCollector collects messages from watchChan and batches them
func (wm *watchManager) runCollector() {
	batches := make([]*Message, 0, maxBatchSize)

	defer func() {
		// drain and send final batch before exiting
		if len(batches) > 0 {
			select {
			case wm.distributeChan <- batches:
			default:
				utils.GetLogger().Printf("[watch_manager] Dropping final batch of %d messages\n", len(batches))
				wm.droppedBatches.Add(1)
				wm.droppedMessages.Add(int64(len(batches)))
			}
		}

		close(wm.distributeChan)
		close(wm.watchChan)
	}()

	sendBatchToDistributor := func(batch []*Message) {
		sendBatch := make([]*Message, len(batch))
		copy(sendBatch, batch)

		// Backpressure: instead of silently dropping the whole batch when the
		// distribution channel is full, wait until the distributor makes room.
		// The collector is an internal worker, so blocking here only applies
		// backpressure to the watch pipeline; write callers still get immediate
		// answers from the buffered watch channel and are never blocked.
		// The only drop path is shutdown, when the worker context is cancelled.
		select {
		case wm.distributeChan <- sendBatch:
		case <-wm.workerCtx.Done():
			utils.GetLogger().Printf("[watch_manager] Dropping batch of %d messages while shutting down\n", len(sendBatch))
			wm.droppedBatches.Add(1)
			wm.droppedMessages.Add(int64(len(sendBatch)))
		}
	}

	for {
		select {
		case msg, ok := <-wm.watchChan:
			if !ok {
				return
			}
			batches = append(batches, msg)
		case <-wm.workerCtx.Done():
			return
		}

	accumulate:
		for {
			if len(batches) >= maxBatchSize {
				sendBatchToDistributor(batches)
				batches = batches[:0]
				break accumulate
			}

			select {
			case msg, ok := <-wm.watchChan:
				if !ok {
					return
				}
				batches = append(batches, msg)
			case <-wm.workerCtx.Done():
				return

			default:
				if len(batches) > 0 {
					sendBatchToDistributor(batches)
					batches = batches[:0]
				}
				break accumulate
			}
		}
	}
}

// runDistributor distributes batches to subscribers
func (wm *watchManager) runDistributor() {
	for {
		select {
		case batch, ok := <-wm.distributeChan:
			if !ok {
				return
			}
			_ = wm.distributeAllMessages(batch)

		case <-wm.workerCtx.Done():
			// drain the distribute channel
			for {
				select {
				case batch, ok := <-wm.distributeChan:
					if !ok {
						return
					}
					_ = wm.distributeAllMessages(batch)
				default:
					return
				}
			}
		}
	}
}

// runVictimCollector collects the victim buckets from the victim channel
// and handle delete bucket operation
// The bucket is deleted only when its all ds bucket are deleted
// we will send the delete bucket message to the subscribers when the bucket is deleted
func (wm *watchManager) runVictimCollector() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case victimBucketToSubscribers, ok := <-wm.victimChan:
			if !ok {
				return
			}

			for identifierId, bucketMap := range victimBucketToSubscribers {
				for key, keyMap := range bucketMap {
					for _, subscriber := range keyMap {
						if subscriber.active.Load() {
							message := NewMessage(subscriber.bucketName, subscriber.key, nil, DataBucketDeleteFlag, uint64(time.Now().Unix()))
							select {
							case subscriber.receiveChan <- message:
							default:
								// drop the message
							}
							close(subscriber.receiveChan)
							subscriber.active.Store(false)
						}
						delete(keyMap, subscriber.id)
					}
					delete(bucketMap, key)
				}
				delete(victimBucketToSubscribers, identifierId)
			}
		case <-ticker.C:
			// avoid busy spinning
		case <-wm.workerCtx.Done():
			return
		}
	}
}

// distribute the messages to the subscribers
func (wm *watchManager) distributeAllMessages(messages []*Message) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	if len(messages) == 0 {
		return nil
	}

	for _, message := range messages {
		bucketMap, ok := wm.lookup[message.BucketName]
		if !ok {
			continue
		}

		if message.Flag == DataBucketDeleteFlag {
			// delete the bucket from the lookup
			wm.deleteBucket(*message)
			continue
		}

		key := message.Key
		subscriberMap, ok := bucketMap[key]
		if !ok {
			continue
		}

		for _, subscriber := range subscriberMap {
			if !subscriber.active.Load() {
				utils.GetLogger().Printf("[watch_manager] Skipping inactive subscriber with id %d for message %s/%s\n", subscriber.id, message.BucketName, message.Key)
				continue
			}

			wm.deliverToSubscriber(message, subscriber)
		}
	}

	return nil
}

// deliverToSubscriber pushes a message to one subscriber, applying backpressure
// instead of dropping on the first full channel:
//   - when the channel has room, the message is delivered immediately;
//   - when it is full, the distributor waits slowSubscriberRetryInterval and
//     retries, so a temporarily slow consumer does not lose events and the
//     messages of a key keep arriving in sequence order;
//   - when the channel stays full across deadMessageThreshold attempts, the
//     subscriber is considered permanently stuck and evicted. This safety
//     valve bounds how long one slow consumer can stall the pipeline (and
//     indirectly the write path); the eviction is logged and accounted in
//     metrics rather than hiding behind a single log line.
func (wm *watchManager) deliverToSubscriber(message *Message, subscriber *subscriber) {
	for {
		select {
		case subscriber.receiveChan <- message:
			subscriber.deadMessages = 0
			wm.deliveredMessages.Add(1)
			return
		default:
		}

		subscriber.deadMessages++
		if subscriber.deadMessages >= deadMessageThreshold {
			wm.evictSubscriber(message, subscriber)
			return
		}

		select {
		case <-wm.workerCtx.Done():
			// shutting down; cleanUpSubscribers closes the channel
			return
		case <-time.After(slowSubscriberRetryInterval):
		}
	}
}

// evictSubscriber removes a permanently stuck subscriber and records the
// discarded message in the backpressure counters.
func (wm *watchManager) evictSubscriber(message *Message, subscriber *subscriber) {
	utils.GetLogger().Printf("[watch_manager] Force-unsubscribing slow subscriber with id %d after %d full-channel attempts, dropping message seq=%d %s/%s\n",
		subscriber.id, subscriber.deadMessages, message.Seq, message.BucketName, message.Key)

	if _, err := wm.findSubscriber(message.BucketName, message.Key, subscriber.id); err == nil {
		delete(wm.lookup[message.BucketName][message.Key], subscriber.id)
		if len(wm.lookup[message.BucketName][message.Key]) == 0 {
			delete(wm.lookup[message.BucketName], message.Key)
		}
		if len(wm.lookup[message.BucketName]) == 0 {
			delete(wm.lookup, message.BucketName)
		}
		if subscriber.active.Load() {
			close(subscriber.receiveChan)
			subscriber.active.Store(false)
		}
	}

	wm.evictedSubscribers.Add(1)
	wm.evictedMessages.Add(1)
}

// subscribe to the key and bucket
// each subscriber has a own channel to receive messages
func (wm *watchManager) subscribe(bucketName core.BucketName, key string) (*subscriber, error) {
	if wm.isClosed() {
		return nil, ErrWatchManagerClosed
	}

	wm.mu.Lock()
	defer wm.mu.Unlock()
	if _, ok := wm.lookup[bucketName]; !ok {
		wm.lookup[bucketName] = make(map[string]map[uint64]*subscriber)
	}

	receiveChan := make(chan *Message, receiveChanBufferSize)

	if _, ok := wm.lookup[bucketName][key]; !ok {
		wm.lookup[bucketName][key] = make(map[uint64]*subscriber)
	}

	id := wm.idGenerator.GenId()
	registeredSubscriber := subscriber{
		id:          id,
		bucketName:  bucketName,
		key:         key,
		receiveChan: receiveChan,
		active:      atomic.Bool{},
	}
	registeredSubscriber.active.Store(true)

	wm.lookup[bucketName][key][id] = &registeredSubscriber

	return &registeredSubscriber, nil
}

// unsubscribe from the key and bucket
func (wm *watchManager) unsubscribe(bucketName core.BucketName, key string, id core.BucketId) error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	subscriber, err := wm.findSubscriber(bucketName, key, id)
	if err != nil {
		return err
	}

	// Clean up the subscriber
	delete(wm.lookup[bucketName][key], id)
	if len(wm.lookup[bucketName][key]) == 0 {
		delete(wm.lookup[bucketName], key)
	}
	if len(wm.lookup[bucketName]) == 0 {
		delete(wm.lookup, bucketName)
	}

	// Close channel if still active
	if subscriber.active.Load() {
		close(subscriber.receiveChan)
		subscriber.active.Store(false)
	}

	return nil
}

func (wm *watchManager) cleanUpSubscribers() {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	for bucket, bucketMap := range wm.lookup {
		for key, keyMap := range bucketMap {
			for _, subscriber := range keyMap {
				if subscriber.active.Load() {
					close(subscriber.receiveChan)
					subscriber.active.Store(false)
				}
				delete(keyMap, subscriber.id)
			}
			delete(bucketMap, key)
		}
		delete(wm.lookup, bucket)
	}
}

func (wm *watchManager) close() error {
	wm.muClosed.Lock()
	defer wm.muClosed.Unlock()

	if wm.closed {
		return ErrWatchManagerClosed
	}

	wm.workerCancel()
	wm.closed = true

	return nil
}

func (wm *watchManager) findSubscriber(bucketName core.BucketName, key string, id uint64) (*subscriber, error) {
	if _, ok := wm.lookup[bucketName]; !ok {
		return nil, ErrBucketSubscriberNotFound
	}
	if _, ok := wm.lookup[bucketName][key]; !ok {
		return nil, ErrKeySubscriberNotFound
	}

	if subscriber, ok := wm.lookup[bucketName][key][id]; ok {
		return subscriber, nil
	}
	return nil, ErrSubscriberNotFound
}

func (wm *watchManager) done() <-chan struct{} {
	return wm.workerCtx.Done()
}

func (wm *watchManager) isClosed() bool {
	wm.muClosed.RLock()
	defer wm.muClosed.RUnlock()

	return wm.closed
}

/*
* delete the buckets from the watch manager
* and notify the subscribers that the keys are deleted due to deleted buckets
* @param deletedbuckets: the buckets to be deleted
 */
func (wm *watchManager) deleteBucket(deletingMessageBucket Message) {
	bucketName := deletingMessageBucket.BucketName

	if _, ok := wm.lookup[bucketName]; !ok {
		return
	}

	identifierId := wm.idGenerator.GenId()
	victimBucketToSubscribers := make(victimBucketToSubscribers)
	victimBucketToSubscribers[identifierId] = wm.lookup[bucketName]
	delete(wm.lookup, bucketName)

	// Log before sending to avoid race condition with victimCollector
	utils.GetLogger().Printf("[watch_manager] Moving bucket %s to victim channel (identifier: %d)\n", bucketName, identifierId)

	// wait for the victim channel to be available
	timeOut := time.After(10 * time.Second)
	for {
		select {
		case <-timeOut:
			utils.GetLogger().Printf("[watch_manager] Timeout sending victim bucket %s to channel\n", bucketName)
			return
		case wm.victimChan <- victimBucketToSubscribers:
			// Successfully sent - victimCollector now owns the map, don't access it anymore
			return
		}
	}
}

// Start starts the watch manager
// Implements Component interface
func (wm *watchManager) Start(ctx context.Context) error {
	if wm.isClosed() {
		return ErrWatchManagerClosed
	}

	wm.muStarted.RLock()
	if wm.started {
		wm.muStarted.RUnlock()
		return nil
	}

	wm.started = true
	wm.muStarted.RUnlock()

	// use a local ready channel to wait for goroutine startup
	ready := make(chan struct{})

	go func() {
		close(ready) // signal that goroutine has started
		wm.startDistributor()
	}()

	// wait for distributor goroutine to start before returning
	select {
	case <-ready:
		utils.GetLogger().Printf("[watch_manager] Watch manager distributor started\n")
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for watch manager distributor to start")
	}
}

// Stop stops the watch manager
// Notifies all subscribers that the database is closing and closes all subscription channels
// Implements Component interface
func (wm *watchManager) Stop(timeout time.Duration) (err error) {
	closeChan := make(chan struct{})

	// close watch manager
	// this cancels context and signals all goroutines to stop
	go func() {
		err = wm.close()
		close(closeChan)
	}()

	select {
	case <-closeChan:
		return err
	case <-time.After(timeout):
		return ErrCloseWatchManagerTimeout
	}
}
