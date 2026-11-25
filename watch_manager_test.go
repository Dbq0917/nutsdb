package nutsdb

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/nutsdb/nutsdb/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startDistributor creates and starts a watch manager with distributor running
func startDistributor(t *testing.T) *watchManager {
	wm := NewWatchManager()
	err := wm.Start(context.Background())
	if err != nil {
		t.Fatalf("failed to start watch manager: %+v", err)
	}

	time.Sleep(100 * time.Millisecond) // Let distributor start
	return wm
}

// wmSubscribe subscribes to a bucket/key and verifies subscription
func wmSubscribe(t *testing.T, wm *watchManager, bucket, key string) (<-chan *Message, uint64) {
	subscriber, err := wm.subscribe(bucket, key)
	require.NoError(t, err)

	wmVerifySubscriberExists(t, wm, bucket, key, subscriber.id)
	return subscriber.receiveChan, subscriber.id
}

// wmUnsubscribe unsubscribes from a bucket/key and verifies removal
func wmUnsubscribe(t *testing.T, wm *watchManager, bucket, key string, id uint64) {
	err := wm.unsubscribe(bucket, key, id)
	require.NoError(t, err)

	wm.mu.Lock()
	if bucketMap, ok := wm.lookup[bucket]; ok {
		assert.NotContains(t, bucketMap, key, "key should be removed from bucket")
	}
	wm.mu.Unlock()
}

// wmSendMessage sends a message and asserts no error
func wmSendMessage(t *testing.T, wm *watchManager, bucket, key string, value []byte) {
	message := NewMessage(bucket, key, value, DataFlag(0), uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityMedium})
	err := wm.sendMessage(message)
	assert.NoError(t, err)
}

// wmSendMessages sends multiple messages to the same bucket/key
func wmSendMessages(t *testing.T, wm *watchManager, bucket, key string, count int) {
	for i := 0; i < count; i++ {
		value := []byte(fmt.Sprintf("value_%d", i))
		message := NewMessage(bucket, key, value, DataFlag(0), uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityMedium})
		err := wm.sendMessage(message)
		assert.NoError(t, err)
	}
}

// wmSendMessages sends multiple messages to the same bucket/key
func wmSendMessagesWithDelay(t *testing.T, wm *watchManager, bucket, key string, count int, delay time.Duration) {
	ticker := time.NewTicker(delay)
	defer ticker.Stop()
	for i := 0; i < count; i++ {
		<-ticker.C
		value := []byte(fmt.Sprintf("value_%d", i))
		wmSendMessage(t, wm, bucket, key, value)
	}
}

// wmReceiveMessage receives a message with timeout and asserts expected values
func wmReceiveMessage(t *testing.T, receiveChan <-chan *Message, expectBucket, expectKey string, timeout time.Duration) *Message {
	select {
	case msg, ok := <-receiveChan:
		if !ok {
			t.Logf("the channel with subscriber id %+v is closed", expectKey)
			return nil
		}
		assert.Equal(t, expectBucket, msg.BucketName)
		assert.Equal(t, expectKey, msg.Key)
		assert.NotNil(t, msg.Value)
		return msg
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for message on %s/%s", expectBucket, expectKey)
		return nil
	}
}

// wmReceiveMessages receives count messages and returns the count received
func wmReceiveMessages(t *testing.T, receiveChan <-chan *Message, expectBucket, expectKey string, expectCount int, timeout time.Duration) int {
	received := 0
	timeoutChan := time.After(timeout)

	for received < expectCount {
		select {
		case msg, ok := <-receiveChan:
			if !ok {
				t.Logf("receive channel closed after receiving %d/%d messages", received, expectCount)
				return received
			}
			assert.Equal(t, expectBucket, msg.BucketName)
			assert.Equal(t, expectKey, msg.Key)
			assert.NotNil(t, msg.Value)
			received++
		case <-timeoutChan:
			t.Logf("timeout after receiving %d/%d messages", received, expectCount)
			return received
		}
	}
	return received
}

// wmDrainChannel drains all messages from a channel until it's closed or timeout
func wmDrainChannel(t *testing.T, receiveChan <-chan *Message, timeout time.Duration) int {
	drained := 0
	timeoutChan := time.After(timeout)

	for {
		select {
		case _, ok := <-receiveChan:
			if !ok {
				return drained
			}
			drained++
		case <-timeoutChan:
			t.Logf("drain timeout after %d messages", drained)
			return drained
		}
	}
}

// wmVerifySubscriberExists checks if a subscriber exists in the lookup table
func wmVerifySubscriberExists(t *testing.T, wm *watchManager, bucket, key string, id core.BucketId) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	assert.Contains(t, wm.lookup, bucket)
	assert.Contains(t, wm.lookup[bucket], key)
	assert.Contains(t, wm.lookup[bucket][key], id)
}

// wmVerifySubscriberRemoved checks if a subscriber is removed from the lookup table
func wmVerifySubscriberRemoved(t *testing.T, wm *watchManager, bucket, key string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if bucketMap, ok := wm.lookup[bucket]; ok {
		assert.NotContains(t, bucketMap, key)
	}
}

// wmVerifyBucketRemoved checks if a bucket is completely removed
func wmVerifyBucketRemoved(t *testing.T, wm *watchManager, bucket string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	assert.NotContains(t, wm.lookup, bucket)
}

// wmStartSender starts a goroutine that sends messages until stopped
func wmStartSender(wm *watchManager, bucket, key string, sent *int, stopChan <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-stopChan:
				return
			case <-wm.done():
				return
			case <-ticker.C:
				value := []byte(fmt.Sprintf("value_%d", sent))
				message := NewMessage(bucket, key, value, DataSetFlag, uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityMedium})
				err := wm.sendMessage(message)
				if err == nil {
					*sent++
				}
			}
		}
	}()
}

// wmStartReceiver starts a goroutine that receives messages from a channel
// Returns a pointer to track the received count
func wmStartReceiver(t *testing.T, receiveChan <-chan *Message, expectBucket, expectKey string, received *int, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for msg := range receiveChan {
			assert.Equal(t, expectBucket, msg.BucketName)
			assert.Equal(t, expectKey, msg.Key)
			assert.NotNil(t, msg.Value)
			assert.Equal(t, DataSetFlag, msg.Flag)
			*received++
		}
	}()
}

// withTestWatchVars temporarily shrinks watch pipeline buffers so backpressure
// engages without sending thousands of messages. The returned function restores
// the original values and must be deferred.
func withTestWatchVars(t *testing.T, overrides map[string]int64) func() {
	t.Helper()

	originals := map[string]int64{
		"watchChanBufferSize":      int64(watchChanBufferSize),
		"receiveChanBufferSize":    int64(receiveChanBufferSize),
		"maxBatchSize":             int64(maxBatchSize),
		"deadMessageThreshold":     int64(deadMessageThreshold),
		"distributeChanBufferSize": int64(distributeChanBufferSize),
		"victimBucketBufferSize":   int64(victimBucketBufferSize),
	}

	applyInt := func(name string, value int64) {
		switch name {
		case "watchChanBufferSize":
			watchChanBufferSize = int(value)
		case "receiveChanBufferSize":
			receiveChanBufferSize = int(value)
		case "maxBatchSize":
			maxBatchSize = int(value)
		case "deadMessageThreshold":
			deadMessageThreshold = int(value)
		case "distributeChanBufferSize":
			distributeChanBufferSize = int(value)
		case "victimBucketBufferSize":
			victimBucketBufferSize = int(value)
		}
	}

	for name, value := range overrides {
		if _, ok := originals[name]; !ok {
			t.Fatalf("unknown watch variable: %s", name)
		}
		applyInt(name, value)
	}

	return func() {
		for name, value := range originals {
			applyInt(name, value)
		}
	}
}

func TestWatchManager_SubscribeAndSendMessage(t *testing.T) {
	t.Run("subscribe and send message", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		bucket := "test"
		key := "test"
		value := []byte("updated value")

		receiveChan, _ := wmSubscribe(t, wm, bucket, key)
		wmSendMessage(t, wm, bucket, key, value)

		msg := wmReceiveMessage(t, receiveChan, bucket, key, 1*time.Second)
		assert.Equal(t, value, msg.Value)
	})

	t.Run("permanently slow subscriber is evicted and the discard is accounted", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		bucket := "bucket_evict"
		key := "key_evict"

		// Subscribe but never consume, so the subscriber stays permanently slow.
		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		// Fill the receive buffer and keep the distributor retrying until the
		// dead-message threshold trips and the safety valve evicts the consumer.
		messagesToSend := receiveChanBufferSize + deadMessageThreshold + 64
		wmSendMessages(t, wm, bucket, key, messagesToSend)

		assert.Eventually(t, func() bool {
			return wm.Metrics().EvictedSubscribers == 1
		}, 10*time.Second, 10*time.Millisecond, "slow subscriber should be force-unsubscribed")

		drained := wmDrainChannel(t, receiveChan, 2*time.Second)
		assert.Equal(t, receiveChanBufferSize, drained, "messages already buffered before eviction are preserved")

		// eviction must be observable through metrics, not a single log line
		metrics := wm.Metrics()
		assert.Equal(t, int64(1), metrics.EvictedSubscribers)
		assert.GreaterOrEqual(t, metrics.EvictedMessages, int64(1))

		wmVerifySubscriberRemoved(t, wm, bucket, key)
		wmVerifyBucketRemoved(t, wm, bucket)
	})

	t.Run("temporarily slow subscriber loses no messages under backpressure", func(t *testing.T) {
		// small buffers so backpressure engages quickly
		restoreWatchVars := withTestWatchVars(t, map[string]int64{
			"watchChanBufferSize":      32,
			"distributeChanBufferSize": 4,
			"receiveChanBufferSize":    16,
		})
		defer restoreWatchVars()

		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		bucket := "bucket_backpressure"
		key := "key_backpressure"
		totalMessages := 400

		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		// the consumer starts slow: let the producer get well ahead of it
		go func() {
			time.Sleep(200 * time.Millisecond)
			for range receiveChan {
			}
		}()

		sendDone := make(chan struct{})
		sendRejected := 0
		go func() {
			defer close(sendDone)
			for i := 0; i < totalMessages; i++ {
				message := NewMessage(bucket, key, []byte(fmt.Sprintf("value_%d", i)), DataSetFlag, uint64(time.Now().Unix()))
				if err := wm.sendMessage(message); err != nil {
					// writes are never blocked: a saturated pipeline answers
					// immediately with an observable, countable rejection
					assert.ErrorIs(t, err, ErrWatchChanCannotSend)
					sendRejected++
				}
			}
		}()

		select {
		case <-sendDone:
		case <-time.After(15 * time.Second):
			t.Fatal("backpressure must not stall the write path forever")
		}

		// accounting invariant: every accepted message is either delivered or
		// explicitly accounted for; nothing disappears behind a single log line
		assert.Eventually(t, func() bool {
			snapshot := wm.Metrics()
			return snapshot.DeliveredMessages == snapshot.AcceptedMessages
		}, 10*time.Second, 10*time.Millisecond,
			"every accepted message must reach the recovered slow subscriber")

		metrics := wm.Metrics()
		assert.Equal(t, int64(sendRejected), metrics.RejectedMessages)
		assert.Equal(t, int64(totalMessages)-int64(sendRejected), metrics.AcceptedMessages)
		assert.Zero(t, metrics.EvictedSubscribers, "a consumer that catches up must not be evicted")
		assert.Zero(t, metrics.DroppedMessages, "no batch may be dropped in steady state")
	})

	t.Run("messages of the same key keep sequence order through backpressure", func(t *testing.T) {
		restoreWatchVars := withTestWatchVars(t, map[string]int64{
			"watchChanBufferSize":      32,
			"distributeChanBufferSize": 4,
			"receiveChanBufferSize":    8,
		})
		defer restoreWatchVars()

		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		bucket := "bucket_order"
		key := "key_order"
		totalMessages := 200

		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		go func() {
			time.Sleep(200 * time.Millisecond)
			for range receiveChan {
			}
		}()

		sendDone := make(chan struct{})
		go func() {
			defer close(sendDone)
			for i := 0; i < totalMessages; i++ {
				message := NewMessage(bucket, key, []byte(fmt.Sprintf("value_%d", i)), DataSetFlag, uint64(time.Now().Unix()))
				_ = wm.sendMessage(message)
			}
		}()

		var lastSeq uint64
		received := 0
		select {
		case <-sendDone:
		case <-time.After(15 * time.Second):
			t.Fatal("backpressure must not stall the write path forever")
		}

		expected := int(wm.Metrics().AcceptedMessages)
		assert.Greater(t, expected, 0)
		for received < expected {
			select {
			case msg, ok := <-receiveChan:
				if !ok {
					t.Fatalf("channel closed after %d/%d messages", received, expected)
				}
				if msg.Seq <= lastSeq {
					t.Fatalf("out-of-order message: seq=%d after %d", msg.Seq, lastSeq)
				}
				lastSeq = msg.Seq
				received++
			case <-time.After(10 * time.Second):
				t.Fatalf("timeout after %d/%d messages", received, expected)
			}
		}

		// writes rejected at ingress never entered the pipeline; every message
		// that did enter must have been delivered, in strictly increasing Seq
		metrics := wm.Metrics()
		assert.Equal(t, metrics.AcceptedMessages, metrics.DeliveredMessages)
		assert.Equal(t, expected, received,
			"every accepted message must be delivered, in sequence order")
	})

	t.Run("multiple subscribers for the same key", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		bucket := "bucket_test"
		key := "key_test"
		numSubscribers := 10
		messagesPerSubscriber := 10

		wg := sync.WaitGroup{}

		// start subscribers goroutines for the same key
		for i := 0; i < numSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				receiveChan, _ := wmSubscribe(t, wm, bucket, key)
				received := wmReceiveMessages(t, receiveChan, bucket, key, messagesPerSubscriber, 5*time.Second)
				assert.Equal(t, messagesPerSubscriber, received)
			}(i)
		}

		// wait for subscribers to register
		time.Sleep(100 * time.Millisecond)

		// start sender goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			wmSendMessages(t, wm, bucket, key, messagesPerSubscriber)
		}()

		wg.Wait()

	})

	t.Run("multiple subscribers", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		numSubscribers := 100
		messagesPerSubscriber := 10

		wg := sync.WaitGroup{}

		// Start 100 subscriber goroutines
		for i := 0; i < numSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				bucket := fmt.Sprintf("bucket_test_%d", i)
				key := fmt.Sprintf("key_test_%d", i)
				receivedCounts := 0

				subscriber, err := wm.subscribe(bucket, key)
				assert.NoError(t, err)

				timeout := time.After(5 * time.Second)

				for {
					select {
					case message, ok := <-subscriber.receiveChan:
						if !ok {
							break
						}

						assert.Equal(t, bucket, message.BucketName)
						assert.Equal(t, key, message.Key)
						assert.NotNil(t, message.Value)

						receivedCounts++

					case <-timeout:
						goto done
					}
				}
			done:
				assert.Equal(t, messagesPerSubscriber, receivedCounts)
			}(i)
		}

		// wait for subscribers to register
		time.Sleep(100 * time.Millisecond)

		// send exactly 10 messages to each subscriber (deterministic)
		for i := 0; i < numSubscribers; i++ {
			for j := 0; j < messagesPerSubscriber; j++ {
				bucket := fmt.Sprintf("bucket_test_%d", i)
				key := fmt.Sprintf("key_test_%d", i)
				value := fmt.Sprintf("value_test_%d_%d", i, j)

				message := NewMessage(bucket, key, []byte(value), DataFlag(0), uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityMedium})
				err := wm.sendMessage(message)
				assert.NoError(t, err)
			}
		}

		// wait for all subscribers to finish (with timeout)
		wg.Wait()
	})

	t.Run("send message and close channel", func(t *testing.T) {
		wm := startDistributor(t)
		isClosed := false

		defer func() {
			if !isClosed {
				_ = wm.close()
			}
		}()

		bucket := "bucket_test"
		key := "key_test"

		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		// Use stopChan to signal sender to stop
		stopChan := make(chan struct{})
		var sentCount, receivedCount int
		senderWg := sync.WaitGroup{}
		receiverWg := sync.WaitGroup{}

		// Start both sender and receiver goroutines
		wmStartReceiver(t, receiveChan, bucket, key, &receivedCount, &receiverWg)
		wmStartSender(wm, bucket, key, &sentCount, stopChan, &senderWg)

		// Let them run for a bit
		time.Sleep(200 * time.Millisecond)

		// CLOSE the stopChan to signal sender to stop (not read from it!)
		close(stopChan)

		// Wait for sender to finish
		senderWg.Wait()

		// Close watch manager and wait for cleanup
		isClosed = true
		_ = wm.close()

		// Wait for the watch manager's internal goroutines to finish and cleanup
		// This ensures cleanUpSubscribers() has run and closed all channels
		wm.wg.Wait()

		// Wait for receiver to finish (channel should now be closed)
		receiverWg.Wait()

		t.Logf("Sent: %d, Received: %d", sentCount, receivedCount)
		assert.Greater(t, sentCount, 0, "should have sent some messages")
		assert.Greater(t, receivedCount, 0, "should have received some messages")
	})
}

func TestWatchManager_SubscribeAndUnsubscribe(t *testing.T) {
	t.Run("subscribe and unsubscribe", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		bucket := "bucket_test"
		key := "key_test"
		expectedMessages := 1000

		receiveChan, id := wmSubscribe(t, wm, bucket, key)

		wg := sync.WaitGroup{}

		// send messages in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			wmSendMessages(t, wm, bucket, key, expectedMessages)
		}()

		// receive messages in goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			received := wmReceiveMessages(t, receiveChan, bucket, key, expectedMessages, 5*time.Second)
			assert.Equal(t, expectedMessages, received)
		}()

		wg.Wait()

		// unsubscribe after receiving all messages
		wmUnsubscribe(t, wm, bucket, key, id)
		wmVerifyBucketRemoved(t, wm, bucket)
	})

	t.Run("subscribe and unsubscribe with multiple keys", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		expectedSubscribers := 10
		expectedMessages := 100
		bucket := "bucket_test"

		// subscribe all keys
		receiveChans := make([]<-chan *Message, expectedSubscribers)
		keys := make([]string, expectedSubscribers)
		ids := make([]core.BucketId, expectedSubscribers)

		for i := 0; i < expectedSubscribers; i++ {
			keys[i] = fmt.Sprintf("key_test_%d", i)
			receiveChans[i], ids[i] = wmSubscribe(t, wm, bucket, keys[i])
		}

		wg := sync.WaitGroup{}

		// start sender goroutines
		for i := 0; i < expectedSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				wmSendMessages(t, wm, bucket, keys[i], expectedMessages)
			}(i)
		}

		// start receiver goroutines with timeout
		for i := 0; i < expectedSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				received := wmReceiveMessages(t, receiveChans[i], bucket, keys[i], expectedMessages, 5*time.Second)
				assert.Equal(t, expectedMessages, received, "subscriber %d should receive all messages", i)
			}(i)
		}

		wg.Wait()

		// unsubscribe all keys
		for i := 0; i < expectedSubscribers; i++ {
			wmUnsubscribe(t, wm, bucket, keys[i], ids[i])
			wmVerifySubscriberRemoved(t, wm, bucket, keys[i])
		}

		// verify bucket is completely removed after all keys unsubscribe
		wmVerifyBucketRemoved(t, wm, bucket)
	})

	t.Run("subscribing and unsubscribe in random time", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		bucket := "bucket_test"
		key0 := "key_test0"
		key1 := "key_test1"
		keys := []string{key0, key1}
		counts := []int{0, 0}
		expectedMessages := 1000

		receiveChan0, subId0 := wmSubscribe(t, wm, bucket, key0)
		receiveChan1, subId1 := wmSubscribe(t, wm, bucket, key1)

		receiveChans := []<-chan *Message{receiveChan0, receiveChan1}

		wg := sync.WaitGroup{}

		for i := range receiveChans {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()

				for counts[i] < expectedMessages {
					msg := wmReceiveMessage(t, receiveChans[i], bucket, keys[i], 10*time.Second)
					if msg == nil {
						return
					}

					counts[i]++
				}
			}(i)
		}

		for i := range keys {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if i == 0 {
					wmSendMessagesWithDelay(t, wm, bucket, keys[i], expectedMessages, 10*time.Millisecond)
				} else {
					wmSendMessages(t, wm, bucket, keys[i], expectedMessages)
				}
			}(i)
		}

		// unsubscribe key1 in random time
		go func() {
			time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
			wmUnsubscribe(t, wm, bucket, keys[0], subId0)
		}()

		wg.Wait()
		wmUnsubscribe(t, wm, bucket, keys[1], subId1)
		wmVerifyBucketRemoved(t, wm, bucket)
		assert.LessOrEqual(t, counts[0], expectedMessages, "should receive less than or equal to expected messages")
		assert.Equal(t, counts[1], expectedMessages, "should receive equal to expected messages")
		t.Logf("key0: %+v, key1: %+v", counts[0], counts[1])
	})
}

func TestWatchManager_StartDistributor(t *testing.T) {
	t.Run("send messages and receive them in batches", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()
		time.Sleep(50 * time.Millisecond)

		bucket := "test_bucket"
		key := "test_key"

		receiveChan, _ := wmSubscribe(t, wm, bucket, key)
		messageCount := 128

		for i := 0; i < messageCount; i++ {
			wmSendMessage(t, wm, bucket, key, []byte(fmt.Sprintf("value_%d", i)))
		}

		received := wmReceiveMessages(t, receiveChan, bucket, key, messageCount, 2*time.Second)
		assert.Equal(t, messageCount, received, "should receive all messages")
	})

	t.Run("max batch size triggers immediate send", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()
		time.Sleep(50 * time.Millisecond)

		bucket := "test_bucket"
		key := "test_key"
		receiveChan, _ := wmSubscribe(t, wm, bucket, key)
		totalMessages := maxBatchSize + maxBatchSize

		for i := 0; i < totalMessages; i++ {
			wmSendMessage(t, wm, bucket, key, []byte(fmt.Sprintf("value_%d", i)))
		}

		received := wmReceiveMessages(t, receiveChan, bucket, key, totalMessages, 3*time.Second)
		assert.Equal(t, totalMessages, received, "both batches must be delivered, no batch dropped")

		_ = wm.close()
	})

	t.Run("send max batch size messages at each subsriber", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		expectedSubscribers := 100
		expectedMessages := 1024
		bucket := "bucket_test"

		// subscribe all keys
		receiveChans := make([]<-chan *Message, expectedSubscribers)
		keys := make([]string, expectedSubscribers)
		ids := make([]core.BucketId, expectedSubscribers)

		for i := 0; i < expectedSubscribers; i++ {
			keys[i] = fmt.Sprintf("key_test_%d", i)
			receiveChans[i], ids[i] = wmSubscribe(t, wm, bucket, keys[i])
		}

		wg := sync.WaitGroup{}

		// start sender goroutines
		// delay sending messages for each subscriber
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for i := 0; i < expectedSubscribers; i++ {
			<-ticker.C
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				wmSendMessages(t, wm, bucket, keys[i], expectedMessages)
			}(i)
		}

		// start receiver goroutines with timeout
		for i := 0; i < expectedSubscribers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				received := wmReceiveMessages(t, receiveChans[i], bucket, keys[i], expectedMessages, 5*time.Second)
				assert.Equal(t, expectedMessages, received, "subscriber %d should receive all messages", i)
			}(i)
		}

		wg.Wait()

		// unsubscribe all keys
		for i := 0; i < expectedSubscribers; i++ {
			wmUnsubscribe(t, wm, bucket, keys[i], ids[i])
			wmVerifySubscriberRemoved(t, wm, bucket, keys[i])
		}

		// verify bucket is completely removed after all keys unsubscribe
		wmVerifyBucketRemoved(t, wm, bucket)
	})

	// Backpressure guarantees that an active consumer loses nothing even when
	// the producer far outruns every channel buffer along the pipeline.
	t.Run("send messages with large number of messages over max watch channel buffer size", func(t *testing.T) {
		restoreWatchVars := withTestWatchVars(t, map[string]int64{
			"watchChanBufferSize":      124,
			"receiveChanBufferSize":    124,
			"distributeChanBufferSize": 16,
		})
		defer restoreWatchVars()

		wm := startDistributor(t)
		defer func() { _ = wm.close() }()
		time.Sleep(50 * time.Millisecond)

		bucket := "test_bucket"
		key := "test_key"
		receiveChan, _ := wmSubscribe(t, wm, bucket, key)
		totalMessages := 2048
		sendingDone := make(chan struct{})
		receivingDone := make(chan struct{})
		sendRejected := 0

		// a consumer that keeps draining the whole time; it receives every
		// message the pipeline accepts, never fewer
		go func() {
			defer close(receivingDone)
			for range receiveChan {
			}
		}()

		// send a large number of messages to the watch channel; when the tiny
		// ingress buffer saturates, writes get an immediate error instead of
		// blocking, and each rejection is accounted
		go func() {
			defer close(sendingDone)

			for i := 0; i < totalMessages; i++ {
				value := []byte(fmt.Sprintf("value_%d", i))
				message := NewMessage(bucket, key, value, DataSetFlag, uint64(time.Now().Unix()))
				err := wm.sendMessage(message)
				if err != nil {
					assert.ErrorIs(t, err, ErrWatchChanCannotSend)
					sendRejected++
				}
			}
		}()

		<-sendingDone

		metrics := wm.Metrics()
		assert.Eventually(t, func() bool {
			return wm.Metrics().DeliveredMessages == wm.Metrics().AcceptedMessages
		}, 10*time.Second, 10*time.Millisecond)

		metrics = wm.Metrics()
		assert.Equal(t, int64(totalMessages), metrics.AcceptedMessages+metrics.RejectedMessages,
			"every write must be either accepted or explicitly rejected")
		assert.Equal(t, int64(sendRejected), metrics.RejectedMessages)
		assert.Equal(t, metrics.AcceptedMessages, metrics.DeliveredMessages,
			"active subscriber must not lose any accepted message")
		assert.Zero(t, metrics.EvictedSubscribers)
		assert.Zero(t, metrics.DroppedMessages)
		_ = wm.close()
		<-receivingDone
	})

	t.Run("sendMessage returns ErrWatchManagerClosed when worker context is done", func(t *testing.T) {
		wm := NewWatchManager()

		for i := 0; i < watchChanBufferSize; i++ {
			wm.watchChan <- NewMessage(
				"bucket",
				"key",
				[]byte("value"),
				DataSetFlag,
				uint64(time.Now().Unix()),
			)
		}

		wm.workerCancel()

		msg := NewMessage("bucket", "key", []byte("value2"), DataSetFlag, uint64(time.Now().Unix()))
		err := wm.sendMessage(msg)
		require.ErrorIs(t, err, ErrWatchManagerClosed)
	})
}

func TestWatchManager_ComponentLifecycle(t *testing.T) {
	wm := NewWatchManager()
	ctx := context.Background()

	require.NoError(t, wm.Start(ctx))
	// Second Start should be a no-op.
	require.NoError(t, wm.Start(ctx))

	bucket := "component_bucket"
	key := "component_key"

	subscriber, err := wm.subscribe(bucket, key)
	require.NoError(t, err)

	msg := NewMessage(bucket, key, []byte("value"), DataSetFlag, uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityMedium})
	require.NoError(t, wm.sendMessage(msg))
	wmReceiveMessage(t, subscriber.receiveChan, bucket, key, 2*time.Second)

	require.NoError(t, wm.Stop(5*time.Second))
	// Stop should be idempotent.
	require.ErrorIs(t, wm.Stop(2*time.Second), ErrWatchManagerClosed)

	// Once stopped, watch manager should reject more work.
	err = wm.sendMessage(msg)
	require.ErrorIs(t, err, ErrWatchManagerClosed)

	require.ErrorIs(t, wm.Start(ctx), ErrWatchManagerClosed)
}

func TestWatchManager_DeleteBucket(t *testing.T) {
	t.Run("delete existing bucket closes channels and removes subscribers", func(t *testing.T) {
		wm := NewWatchManager()
		// Start both collector and distributor goroutines
		go wm.startDistributor()
		defer func() { _ = wm.close() }()

		// Give goroutines time to start
		time.Sleep(50 * time.Millisecond)

		bucket := core.BucketName("bucket_test")
		keys := []string{"key1", "key2"}

		type subscriberInfo struct {
			ch  <-chan *Message
			id  uint64
			key string
		}
		var subscribers []subscriberInfo

		// Subscribe multiple times to different keys
		for _, key := range keys {
			for i := 0; i < 2; i++ {
				subscriber, err := wm.subscribe(bucket, key)
				require.NoError(t, err)
				subscribers = append(subscribers, subscriberInfo{ch: subscriber.receiveChan, id: subscriber.id, key: key})
			}
		}

		// Verify all subscribers are active
		wm.mu.Lock()
		bucketMap, ok := wm.lookup[bucket]
		wm.mu.Unlock()
		require.True(t, ok, "bucket should exist in lookup")
		assert.Len(t, bucketMap, len(keys), "bucket should have correct number of keys")

		// Send bucket delete message
		deleteMsg := NewMessage(bucket, "", nil, DataBucketDeleteFlag,
			uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityHigh})
		err := wm.sendMessage(deleteMsg)
		require.NoError(t, err)

		// Wait for processing
		time.Sleep(200 * time.Millisecond)

		// Bucket should be removed from lookup
		wm.mu.Lock()
		_, ok = wm.lookup[bucket]
		wm.mu.Unlock()
		assert.False(t, ok, "bucket should be removed from lookup")

		// All subscribers should receive a delete message, then channel closes
		for _, sub := range subscribers {
			select {
			case msg, ok := <-sub.ch:
				if ok {
					assert.Equal(t, DataBucketDeleteFlag, msg.Flag, "should receive delete flag")
					assert.Equal(t, bucket, msg.BucketName, "should match bucket name")
					assert.Equal(t, sub.key, msg.Key, "should match subscriber key")
				}
			case <-time.After(1 * time.Second):
				t.Fatalf("timeout waiting for delete message on subscriber %d", sub.id)
			}

			// Channel should be closed
			select {
			case _, ok := <-sub.ch:
				assert.False(t, ok, "subscriber channel should be closed")
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("timeout waiting for channel close on subscriber %d", sub.id)
			}
		}
	})

	t.Run("delete non-existent bucket is no-op", func(t *testing.T) {
		wm := NewWatchManager()
		go wm.startDistributor()
		defer func() { _ = wm.close() }()

		time.Sleep(50 * time.Millisecond)

		bucket := core.BucketName("bucket1")
		key := "key1"
		subscriber, err := wm.subscribe(bucket, key)
		require.NoError(t, err)

		// Send delete for an unrelated bucket
		deleteMsg := NewMessage("otherBucket", "", nil, DataBucketDeleteFlag,
			uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityHigh})
		err = wm.sendMessage(deleteMsg)
		require.NoError(t, err)

		// Wait for processing
		time.Sleep(200 * time.Millisecond)

		// Original subscriber should still exist and be active
		wm.mu.Lock()
		sub, err := wm.findSubscriber(bucket, key, subscriber.id)
		wm.mu.Unlock()
		require.NoError(t, err)
		assert.True(t, sub.active.Load(), "subscriber should remain active")

		// Channel should not be closed
		select {
		case msg, ok := <-subscriber.receiveChan:
			if !ok {
				t.Fatal("channel should not be closed for unrelated bucket deletion")
			}
			t.Fatalf("unexpected message received: %+v", msg)
		case <-time.After(100 * time.Millisecond):
			// Expected - no message received
		}
	})

	t.Run("delete bucket with no subscribers is no-op", func(t *testing.T) {
		wm := NewWatchManager()
		go wm.startDistributor()
		defer func() { _ = wm.close() }()

		time.Sleep(50 * time.Millisecond)

		// Send delete for a bucket with no subscribers
		deleteMsg := NewMessage("nonExistentBucket", "", nil, DataBucketDeleteFlag,
			uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityHigh})
		err := wm.sendMessage(deleteMsg)
		require.NoError(t, err)

		// Wait for processing - should not panic or error
		time.Sleep(200 * time.Millisecond)
	})

	t.Run("multiple bucket deletions handled correctly", func(t *testing.T) {
		wm := NewWatchManager()
		go wm.startDistributor()
		defer func() { _ = wm.close() }()

		time.Sleep(50 * time.Millisecond)

		bucket1 := core.BucketName("bucket1")
		bucket2 := core.BucketName("bucket2")

		// Subscribe to both buckets
		subscriber1, err := wm.subscribe(bucket1, "key1")
		require.NoError(t, err)
		subscriber2, err := wm.subscribe(bucket2, "key2")
		require.NoError(t, err)

		// Delete both buckets
		deleteMsg1 := NewMessage(bucket1, "", nil, DataBucketDeleteFlag,
			uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityHigh})
		err = wm.sendMessage(deleteMsg1)
		require.NoError(t, err)

		deleteMsg2 := NewMessage(bucket2, "", nil, DataBucketDeleteFlag,
			uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityHigh})
		err = wm.sendMessage(deleteMsg2)
		require.NoError(t, err)

		// Wait for processing
		time.Sleep(300 * time.Millisecond)

		// Both buckets should be removed
		wm.mu.Lock()
		_, ok1 := wm.lookup[bucket1]
		_, ok2 := wm.lookup[bucket2]
		wm.mu.Unlock()
		assert.False(t, ok1, "bucket1 should be removed")
		assert.False(t, ok2, "bucket2 should be removed")

		// Both channels should receive delete and close
		for i, ch := range []<-chan *Message{subscriber1.receiveChan, subscriber2.receiveChan} {
			select {
			case msg, ok := <-ch:
				if ok {
					assert.Equal(t, DataBucketDeleteFlag, msg.Flag)
				}
			case <-time.After(1 * time.Second):
				t.Fatalf("timeout on bucket %d", i+1)
			}

			_, ok := <-ch
			assert.False(t, ok, fmt.Sprintf("channel %d should be closed", i+1))
		}
	})

	t.Run("new subscription after bucket deletion uses fresh state", func(t *testing.T) {
		wm := NewWatchManager()
		go wm.startDistributor()
		defer func() { _ = wm.close() }()

		time.Sleep(50 * time.Millisecond)

		bucket := core.BucketName("reusable_bucket")
		key := "key1"

		// First subscription
		subscriber1, err := wm.subscribe(bucket, key)
		require.NoError(t, err)

		// Delete bucket
		deleteMsg := NewMessage(bucket, "", nil, DataBucketDeleteFlag,
			uint64(time.Now().Unix()), MessageOptions{Priority: MessagePriorityHigh})
		err = wm.sendMessage(deleteMsg)
		require.NoError(t, err)

		// Wait for deletion to complete
		time.Sleep(200 * time.Millisecond)

		// Drain and verify first channel is closed
		for {
			if _, ok := <-subscriber1.receiveChan; !ok {
				break
			}
		}

		// New subscription to same bucket/key should work
		subscriber2, err := wm.subscribe(bucket, key)
		require.NoError(t, err)
		assert.NotNil(t, subscriber2.receiveChan)

		// New subscriber should exist and be active
		wm.mu.Lock()
		sub, err := wm.findSubscriber(bucket, key, subscriber2.id)
		wm.mu.Unlock()
		require.NoError(t, err)
		assert.True(t, sub.active.Load(), "new subscriber should be active")

		// Send a regular update message to verify new subscription works
		updateMsg := NewMessage(bucket, key, []byte("test_value"), DataSetFlag, uint64(time.Now().Unix()))
		err = wm.sendMessage(updateMsg)
		require.NoError(t, err)

		// New subscriber should receive the update
		select {
		case msg, ok := <-subscriber2.receiveChan:
			require.True(t, ok, "new subscriber should receive message")
			assert.Equal(t, DataSetFlag, msg.Flag)
			assert.Equal(t, []byte("test_value"), msg.Value)
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for message on new subscription")
		}
	})
}

func TestWatchManager_SendMessage(t *testing.T) {
	t.Run("should send normal priority message successfully", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() {
			err := wm.close()
			assert.NoError(t, err)
		}()

		bucket := "test_bucket"
		key := "test_key"
		value := []byte("test_value")

		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		// Send normal priority message
		msg := NewMessage(bucket, key, value, DataSetFlag, uint64(time.Now().Unix()))
		err := wm.sendMessage(msg)
		require.NoError(t, err)

		// Verify message is received
		select {
		case received := <-receiveChan:
			assert.Equal(t, bucket, received.BucketName)
			assert.Equal(t, key, received.Key)
			assert.Equal(t, value, received.Value)
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})

	t.Run("should send high priority message successfully", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() {
			err := wm.close()
			assert.NoError(t, err)
		}()

		bucket := "test_bucket"
		key := "test_key"
		value := []byte("high_priority_value")

		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		// Send high priority message
		msg := NewMessage(bucket, key, value, DataSetFlag, uint64(time.Now().Unix()),
			MessageOptions{Priority: MessagePriorityHigh})
		err := wm.sendMessage(msg)
		require.NoError(t, err)

		// Verify message is received
		select {
		case received := <-receiveChan:
			assert.Equal(t, bucket, received.BucketName)
			assert.Equal(t, key, received.Key)
			assert.Equal(t, value, received.Value)
		case <-time.After(1 * time.Second):
			t.Fatal("timeout waiting for high priority message")
		}
	})

	t.Run("should return error when watch manager is closed", func(t *testing.T) {
		wm := NewWatchManager()

		// Close the watch manager first
		wm.muClosed.Lock()
		wm.closed = true
		wm.muClosed.Unlock()

		msg := NewMessage("bucket", "key", []byte("value"), DataSetFlag, uint64(time.Now().Unix()))
		err := wm.sendMessage(msg)

		assert.ErrorIs(t, err, ErrWatchManagerClosed)
	})

	t.Run("should return error when watch channel is full", func(t *testing.T) {
		// Create a new watch manager with small buffer for testing
		wm := NewWatchManager()

		// Fill the watch channel to capacity
		for i := 0; i < watchChanBufferSize; i++ {
			wm.watchChan <- NewMessage("bucket", "key", []byte("value"), DataSetFlag, uint64(time.Now().Unix()))
		}

		// Try to send another message (should fail immediately due to full channel)
		msg := NewMessage("bucket", "key", []byte("value"), DataSetFlag, uint64(time.Now().Unix()))
		err := wm.sendMessage(msg)

		assert.ErrorIs(t, err, ErrWatchChanCannotSend)
	})

	t.Run("should return error when worker context is cancelled", func(t *testing.T) {
		wm := NewWatchManager()

		// Fill the watch channel
		for i := 0; i < watchChanBufferSize; i++ {
			wm.watchChan <- NewMessage("bucket", "key", []byte("value"), DataSetFlag, uint64(time.Now().Unix()))
		}

		// Cancel the worker context
		wm.workerCancel()

		// Try to send message - should detect context cancellation
		msg := NewMessage("bucket", "key", []byte("value"), DataSetFlag, uint64(time.Now().Unix()))
		err := wm.sendMessage(msg)

		assert.ErrorIs(t, err, ErrWatchManagerClosed)
	})
}

func TestWatchManager_Stop(t *testing.T) {
	t.Run("should stop successfully with active subscribers", func(t *testing.T) {
		wm := NewWatchManager()
		err := wm.Start(context.Background())
		require.NoError(t, err)

		// Add some subscribers
		bucket := "test_bucket"
		key1 := "key1"
		key2 := "key2"

		subscriber1, err := wm.subscribe(bucket, key1)
		require.NoError(t, err)
		subscriber2, err := wm.subscribe(bucket, key2)
		require.NoError(t, err)

		// Stop should succeed
		err = wm.Stop(5 * time.Second)
		assert.NoError(t, err)

		// Verify subscribers' channels are closed
		_, ok1 := <-subscriber1.receiveChan
		_, ok2 := <-subscriber2.receiveChan
		assert.False(t, ok1, "subscriber1 channel should be closed")
		assert.False(t, ok2, "subscriber2 channel should be closed")
	})

	t.Run("should stop successfully when no subscribers", func(t *testing.T) {
		wm := NewWatchManager()
		err := wm.Start(context.Background())
		require.NoError(t, err)

		// Stop immediately without any subscribers
		err = wm.Stop(2 * time.Second)
		assert.NoError(t, err)

		// Verify watch manager is closed
		assert.True(t, wm.isClosed())
	})

	t.Run("should return error when stopping already stopped manager", func(t *testing.T) {
		wm := NewWatchManager()
		err := wm.Start(context.Background())
		require.NoError(t, err)

		// First stop should succeed
		err = wm.Stop(2 * time.Second)
		require.NoError(t, err)

		// Second stop should return error
		err = wm.Stop(2 * time.Second)
		assert.ErrorIs(t, err, ErrWatchManagerClosed)
	})
}

func TestWatchManager_Metrics(t *testing.T) {
	t.Run("healthy pipeline reports zero loss and full accounting", func(t *testing.T) {
		wm := startDistributor(t)
		defer func() { _ = wm.close() }()

		bucket := "metrics_bucket"
		key := "metrics_key"
		receiveChan, _ := wmSubscribe(t, wm, bucket, key)

		const messageCount = 50
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range receiveChan {
			}
		}()

		wmSendMessages(t, wm, bucket, key, messageCount)

		assert.Eventually(t, func() bool {
			return wm.Metrics().DeliveredMessages == messageCount
		}, 5*time.Second, 10*time.Millisecond)

		metrics := wm.Metrics()
		assert.Equal(t, int64(messageCount), metrics.AcceptedMessages)
		assert.Equal(t, int64(messageCount), metrics.DeliveredMessages)
		assert.Zero(t, metrics.RejectedMessages)
		assert.Zero(t, metrics.DroppedBatches)
		assert.Zero(t, metrics.DroppedMessages)
		assert.Zero(t, metrics.EvictedSubscribers)
		assert.Zero(t, metrics.EvictedMessages)

		_ = wm.close()
		<-done
	})

	t.Run("ingress rejection is counted without blocking", func(t *testing.T) {
		wm := NewWatchManager() // not started: watchChan never drains

		for i := 0; i < watchChanBufferSize; i++ {
			require.NoError(t, wm.sendMessage(NewMessage(
				"bucket", "key", []byte("value"), DataSetFlag, uint64(time.Now().Unix()))))
		}

		err := wm.sendMessage(NewMessage(
			"bucket", "key", []byte("overflow"), DataSetFlag, uint64(time.Now().Unix())))
		require.ErrorIs(t, err, ErrWatchChanCannotSend)

		metrics := wm.Metrics()
		assert.Equal(t, int64(watchChanBufferSize), metrics.AcceptedMessages)
		assert.Equal(t, int64(1), metrics.RejectedMessages)
		assert.Zero(t, metrics.DeliveredMessages)
	})

	t.Run("metrics remain readable after shutdown", func(t *testing.T) {
		wm := NewWatchManager()
		require.NoError(t, wm.Start(context.Background()))

		sent := 0
		for i := 0; i < watchChanBufferSize; i++ {
			if err := wm.sendMessage(NewMessage(
				"bucket", "key", []byte("value"), DataSetFlag, uint64(time.Now().Unix()))); err == nil {
				sent++
			}
		}

		require.NoError(t, wm.Stop(5*time.Second))

		metrics := wm.Metrics()
		assert.Equal(t, int64(sent), metrics.AcceptedMessages)
		assert.Equal(t, int64(0), metrics.EvictedSubscribers)
		if metrics.DroppedBatches > 0 {
			assert.Greater(t, metrics.DroppedMessages, int64(0),
				"dropped batches must also report the discarded message count")
		}
	})
}
