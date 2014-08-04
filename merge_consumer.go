package mqhammer

import (
	"sync"
)

type MergeConsumer struct {
	Consumers []Consumer
	first     error
	once      sync.Once
}

// Consume returns a channel of Delivery. Once the channel is closed,
// you should check consumer.Err() to see if it was terminated because
// of an error. Pass in a done channel to signal when to stop consuming
// by closing done.
func (c *MergeConsumer) Consume() <-chan Delivery {
	var wg sync.WaitGroup
	out := make(chan Delivery)

	for _, consumer := range c.Consumers {
		wg.Add(1)
		go func(consumer Consumer) {
			for d := range consumer.Consume() {
				out <- d
			}
			if err := consumer.Err(); err != nil {
				if c.first == nil {
					c.Stop()
					c.first = err
				}
			}
			wg.Done()
		}(consumer)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func (c MergeConsumer) Err() error {
	return c.first
}

func (c *MergeConsumer) Stop() {
	c.once.Do(func() {
		for _, consumer := range c.Consumers {
			consumer.Stop()
		}
	})
}
