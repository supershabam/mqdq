package mqhammer

import (
	"sync"
)

type Merger struct {
	Consumers []Consumer
	first     error
}

func NewMerger(consumers ...Consumer) *Merger {
	return &Merger{
		Consumers: consumers,
	}
}

// Consume calls consume on all passed in consumers and forwards their
// deliveries to the returned outut channel. Once all sub-consumer
// output channels are closed, this channel will close. If a sub-consumer's
// output channel closes AND has an error, Merger will set its error
// to this error and call Stop on all sub-consumers.
func (c *Merger) Consume() <-chan Delivery {
	var wg sync.WaitGroup
	out := make(chan Delivery)

	consume := func(consumer Consumer) {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for d := range consumer.Consume() {
				out <- d
			}
			if err := consumer.Err(); err != nil && c.first == nil {
				c.first = err
				c.Stop()
			}
		}()
	}

	for _, consumer := range c.Consumers {
		consume(consumer)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Err returns the first error encountered by a sub consumer. It
// should be called and checked after the Consume channel is closed.
func (c Merger) Err() error {
	return c.first
}

// Stop signals consumption to end. This method returns immediately, and
// once the Consume channel closes, the consumption has come to a full
// stop.
func (c *Merger) Stop() {
	for _, consumer := range c.Consumers {
		consumer.Stop()
	}
}
