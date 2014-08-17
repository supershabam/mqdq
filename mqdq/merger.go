package mqdq

import (
	"sync"
)

// Merger consumes from many consumers in parallel and outputs the consume stream
// in one output. If one consumer errors, then all other consumers are stopped and
// drained.
type Merger struct {
	Consumers []Consumer
	first     error
}

// Consume calls consume on all passed in consumers and forwards their
// deliveries to the returned outut channel. Once all sub-consumer
// output channels are closed, this channel will close. If a sub-consumer's
// output channel closes AND has an error, Merger will set its error
// to this error and call Stop on all sub-consumers.
func (m *Merger) Consume() <-chan Delivery {
	out := make(chan Delivery)

	var wg sync.WaitGroup
	var lock sync.Mutex
	wg.Add(len(m.Consumers))

	merge := func(c Consumer) {
		for d := range c.Consume() {
			out <- d
		}
		// only set m.first once with concurrent mergers
		lock.Lock()
		defer lock.Unlock()
		if err := c.Err(); err != nil && m.first == nil {
			m.first = err
			m.Stop()
		}
		wg.Done()
	}

	go func() {
		defer close(out)

		for _, c := range m.Consumers {
			go merge(c)
		}

		wg.Wait()
	}()

	return out
}

// Err returns the first error encountered by a sub consumer. It
// should be called and checked after the Consume channel is closed.
func (m Merger) Err() error {
	return m.first
}

// Stop signals consumption to end. This method returns immediately, and
// once the Consume channel closes, the consumption has come to a full
// stop.
func (m *Merger) Stop() {
	for _, c := range m.Consumers {
		c.Stop()
	}
}
