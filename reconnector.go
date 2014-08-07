package mqhammer

import (
	"net"
	"sync"
	"time"
)

type Reconnector struct {
	c       Consumer
	d       time.Duration
	done    chan struct{}
	nc      NewConsumer
	err     error
	once    *sync.Once
	stopped bool
}

func NewReconnector(nc NewConsumer, d time.Duration) *Reconnector {
	return &Reconnector{
		nc:   nc,
		d:    d,
		done: make(chan struct{}),
		once: &sync.Once{},
	}
}

// Consume uses the NewConsumer function to create a Connector. If ever
// the error returned is a type of net.Error and is Temporary(), then
// the error will be supressed, a new consumer will be created, and the
// cycle will continue. Otherwise, the error will be recorded as the
// reconnector's error, and the output channel will be closed.
func (r *Reconnector) Consume() <-chan Delivery {
	out := make(chan Delivery)
	go func() {
		defer close(out)

		for {

			select {
			case <-r.done:
				return
			default:
			}
			c, err := r.nc()
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				time.Sleep(r.d)
				continue
			}
			if err != nil {
				r.err = err
				return
			}
			r.c = c
			for d := range c.Consume() {
				out <- d
			}
			if err := c.Err(); err != nil {
				r.err = err
				return
			}
		}
	}()
	return out
}

// Err returns the error encountered by the sub consumer.
func (r Reconnector) Err() error {
	return r.err
}

// Stop tells the sub consumer to stop (or if we're sleeping to stop)
func (r *Reconnector) Stop() {
	r.once.Do(func() {
		r.stopped = true
		close(r.done)
	})
}
