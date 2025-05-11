package subpub

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_SubscribeAndPublish(t *testing.T) {
	os.Setenv("SUBSCRIBERS_CHANNEL_CAPACITY", "10")
	sp := NewSubPub()

	wg := sync.WaitGroup{}
	wg.Add(1)

	_, err := sp.Subscribe("test", func(msg interface{}) {
		assert.Equal(t, "test message", msg)
		wg.Done()
	})
	require.NoError(t, err)

	err = sp.Publish("test", "test message")
	require.NoError(t, err)

	wg.Wait()
}

func Test_ManySubscribe(t *testing.T) {
	os.Setenv("SUBSCRIBERS_CHANNEL_CAPACITY", "10")
	sp := NewSubPub()

	wg := sync.WaitGroup{}

	for range 20 {
		wg.Add(1)
		_, err := sp.Subscribe("test", func(msg interface{}) {
			assert.Equal(t, "test message", msg)
			wg.Done()
		})
		require.NoError(t, err)
	}

	err := sp.Publish("test", "test message")
	require.NoError(t, err)

	wg.Done()
}

func Test_ManyTopics(t *testing.T) {
	os.Setenv("SUBSCRIBERS_CHANNEL_CAPACITY", "20")
	sp := NewSubPub()

	wg := sync.WaitGroup{}

	for i := range 20 {
		wg.Add(1)
		number := strconv.Itoa(i + 1)
		_, err := sp.Subscribe("test"+number, func(msg interface{}) {
			assert.Equal(t, "test message "+number, msg)
			wg.Done()
		})
		require.NoError(t, err)
	}

	for i := range 20 {
		number := strconv.Itoa(i + 1)
		err := sp.Publish("test"+number, "test message "+number)
		require.NoError(t, err)
	}

	wg.Wait()
}

func Test_ManyPublish(t *testing.T) {
	os.Setenv("SUBSCRIBERS_CHANNEL_CAPACITY", "20")
	sp := NewSubPub()

	wg := sync.WaitGroup{}

	_, err := sp.Subscribe("test", func(msg interface{}) {
		assert.Equal(t, "test message", msg)
		wg.Done()
	})
	require.NoError(t, err)

	for range 20 {
		wg.Add(1)
		err := sp.Publish("test", "test message")
		require.NoError(t, err)
	}

	wg.Wait()
}

func Test_ManyParallelPublish(t *testing.T) {
	os.Setenv("SUBSCRIBERS_CHANNEL_CAPACITY", "20")
	sp := NewSubPub()

	wg := sync.WaitGroup{}

	_, err := sp.Subscribe("test", func(msg interface{}) {
		assert.Equal(t, "test message", msg)
		wg.Done()
	})
	require.NoError(t, err)

	for range 20 {
		wg.Add(1)
		go func() {
			err := sp.Publish("test", "test message")
			require.NoError(t, err)
		}()
	}

	wg.Wait()
}

func Test_NonBlockPublish(t *testing.T) {
	os.Setenv("SUBSCRIBERS_CHANNEL_CAPACITY", "10")
	sp := NewSubPub()

	wg := sync.WaitGroup{}

	_, err := sp.Subscribe("test", func(msg interface{}) {
		time.Sleep(20 * time.Millisecond)
		wg.Done()
	})
	require.NoError(t, err)

	doneCh := make(chan struct{})
	go func() {
		for range 20 {
			wg.Add(1)
			err = sp.Publish("test", "test message")
			require.NoError(t, err)
		}
		close(doneCh)
	}()

	select {
	case <-time.After(20 * time.Second):
		t.FailNow()
	case <-doneCh:
	}
}

func Test_SubscribeAfterCLose(t *testing.T) {
	os.Setenv("SUBSCRIBERS_CHANNEL_CAPACITY", "20")
	sp := NewSubPub()

	err := sp.Close(context.Background())
	require.NoError(t, err)

	_, err = sp.Subscribe("test", func(msg interface{}) {
		t.Error("should not be called")
	})
	require.EqualError(t, ErrClosed, err.Error())
}

func Test_PublishAfterClose(t *testing.T) {
	os.Setenv("SUBSCRIBERS_CHANNEL_CAPACITY", "20")
	sp := NewSubPub()

	err := sp.Close(context.Background())
	require.NoError(t, err)

	err = sp.Publish("test", "test message")
	require.EqualError(t, ErrClosed, err.Error())
}

func Test_Unsubscribe(t *testing.T) {
	os.Setenv("SUBSCRIBERS_CHANNEL_CAPACITY", "10")
	sp := NewSubPub()

	wg := sync.WaitGroup{}
	wg.Add(1)

	_, err := sp.Subscribe("test", func(msg interface{}) {
		assert.Equal(t, "test message", msg)
		wg.Done()
	})
	require.NoError(t, err)

	s, err := sp.Subscribe("test", func(msg interface{}) {
		t.Error("should not be called")
	})
	require.NoError(t, err)

	s.Unsubscribe()

	err = sp.Publish("test", "test message")
	require.NoError(t, err)
	wg.Wait()
}
