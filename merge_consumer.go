package mqhammer

import (
	"sync"
)

type MergeConsumer struct {
	Consumers []Consumer
	first       error
}

// Consume calls consume on all child consumers and merges their output
// into one channel. If any child ends the stream and has an error, this
// channel is closed and the consumer is set with that error.
func (c *MergeConsumer) Consume(done <-chan struct{}) <-chan Delivery{
  doneChildren := make(chan struct{})
  once := sync.Once{}
  doCloseChildren := func() {
    once.Do(func() {
      close(doneChildren)
    })
  }
  
	wg := sync.WaitGroup{}
	out := make(chan Delivery)
	go func() {
		defer close(out)
    for i, in := ins {
      wg.Add()
      go func(in <-chan Delivery) {
        for d := range in {
          out <- d
        }
        wg.Done()
      }(in)
    }
		wg.Wait()
	}()
	return out, nil
}

func (c MergeConsumer) Err() error {
	return c.first
}
