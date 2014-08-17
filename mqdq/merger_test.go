package mqdq

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

type DelayValue struct {
	Delay time.Duration
	Value []byte
}

type NoopAcknowledger struct{}

func (a NoopAcknowledger) Ack()  {}
func (a NoopAcknowledger) Nack() {}

// PlaybackConsumer will play through provided delay values sleeping and then
// outputing the value on the consume channel until all the delay values are
// read. Consuming mutates the DelayValues so that we can see afterwards how
// many were still left to be consumed when stopped
type PlaybackConsumer struct {
	DelayValues []DelayValue
	Error       error
	done        chan struct{}
	once        sync.Once
}

func NewPlaybackConsumer(delayValues []DelayValue) *PlaybackConsumer {
	return &PlaybackConsumer{
		DelayValues: delayValues,
		done:        make(chan struct{}),
		once:        sync.Once{},
	}
}

func (c *PlaybackConsumer) Consume() <-chan Delivery {
	out := make(chan Delivery)
	go func() {
		defer close(out)

		for {
			if len(c.DelayValues) == 0 {
				return
			}
			select {
			case <-c.done:
				return
			case <-time.After(c.DelayValues[0].Delay):
				out <- Delivery{
					Ackr: NoopAcknowledger{},
					Msg:  c.DelayValues[0].Value,
				}
				c.DelayValues = c.DelayValues[1:]
			}
		}
	}()
	return out
}

func (c *PlaybackConsumer) Err() error {
	return c.Error
}

func (c *PlaybackConsumer) Stop() {
	c.once.Do(func() {
		close(c.done)
	})

}

func TestMergerConsumer(t *testing.T) {
	// test merging and finish to completion
	// 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
	//  1,  2,  1,  3,  1,  2,  1,  3,  1,   2
	c := Merger{
		Consumers: []Consumer{
			NewPlaybackConsumer([]DelayValue{
				DelayValue{10 * time.Millisecond, []byte("1")},
				DelayValue{20 * time.Millisecond, []byte("3")},
				DelayValue{20 * time.Millisecond, []byte("5")},
				DelayValue{20 * time.Millisecond, []byte("7")},
				DelayValue{20 * time.Millisecond, []byte("9")},
			}),
			NewPlaybackConsumer([]DelayValue{
				DelayValue{20 * time.Millisecond, []byte("2")},
				DelayValue{40 * time.Millisecond, []byte("6")},
				DelayValue{40 * time.Millisecond, []byte("10")},
			}),
			NewPlaybackConsumer([]DelayValue{
				DelayValue{40 * time.Millisecond, []byte("4")},
				DelayValue{40 * time.Millisecond, []byte("8")},
			}),
		},
	}
	count := 1
	for d := range c.Consume() {
		expect := []byte(fmt.Sprintf("%d", count))
		if !bytes.Equal(d.Msg, expect) {
			t.Errorf("expected %s, but got %s", expect, d.Msg)
		}
		count++
		d.Ack()
	}
	if err := c.Err(); err != nil {
		t.Error("consumer finished with error")
	}

	// test stopping
	err1 := fmt.Errorf("err1")
	// err2 := fmt.Errorf("err2")
	// err3 := fmt.Errorf("err3")
	c1 := NewPlaybackConsumer([]DelayValue{
		DelayValue{10 * time.Millisecond, []byte("1")},
		DelayValue{20 * time.Millisecond, []byte("3")},
		DelayValue{20 * time.Millisecond, []byte("5")},
		DelayValue{20 * time.Millisecond, []byte("7")},
		DelayValue{20 * time.Millisecond, []byte("9")},
	})
	c2 := NewPlaybackConsumer([]DelayValue{
		DelayValue{20 * time.Millisecond, []byte("2")},
		DelayValue{40 * time.Millisecond, []byte("6")},
		DelayValue{40 * time.Millisecond, []byte("10")},
	})
	c3 := NewPlaybackConsumer([]DelayValue{
		DelayValue{40 * time.Millisecond, []byte("4")},
		DelayValue{40 * time.Millisecond, []byte("8")},
	})
	c = Merger{
		Consumers: []Consumer{c1, c2, c3},
	}

	time.AfterFunc(45*time.Millisecond, func() {
		c1.Error = err1
		c1.Stop()
	})

	count = 1
	for d := range c.Consume() {
		expect := []byte(fmt.Sprintf("%d", count))
		if !bytes.Equal(d.Msg, expect) {
			t.Errorf("expected %s, but got %s", expect, d.Msg)
		}
		count++
		d.Ack()
	}
	if err := c.Err(); err != err1 {
		t.Error("expected consumer to error with %s, but ended with %s", err1, c.Err())
	}
	if len(c1.DelayValues) != 3 {
		t.Errorf("expected 3 remaining delay values in c1 but have %v", c1.DelayValues)
	}
	if len(c2.DelayValues) != 2 {
		t.Errorf("expected 2 remaining delay values in c2 but have %v", c2.DelayValues)
	}
	if len(c3.DelayValues) != 1 {
		t.Errorf("expected 1 remaining delay values in c3 but have %v", c3.DelayValues)
	}

}
