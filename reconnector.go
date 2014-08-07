package mqhammer

import (
	"net"
	"sync"
	"time"
)

type Reconnector struct {
	err  error
	d    time.Duration
	done chan struct{}
	nc   NewConsumer
	once *sync.Once
}

func NewReconnector(nc NewConsumer, d time.Duration) *Reconnector {
	return &Reconnector{
		d:    d,
		nc:   nc,
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

	Loop:
		c, err := r.nc()
		if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
			select {
			case <-r.done:
				return
			case <-time.After(r.d):
				goto Loop
			}
		}
		if err != nil {
			r.err = err
			return
		}
		in := c.Consume()
		for {
			select {
			case <-r.done:
				c.Stop()
			case d, ok := <-in:
				if !ok {
					err := c.Err()
					if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
						goto Loop
					}
					if err != nil {
						r.err = err
					}
					goto Loop
				}
				out <- d
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
		close(r.done)
	})
}
