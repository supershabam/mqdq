package mqhammer

type ReconnectingConsumer struct {
	consumer Consumer
	dial     func() (Consumer, error)
	err      error
}

func NewReconnectingConsumer(dial func() (Consumer, error)) *ReconnectingConsumer {
	return &ReconnectingConsumer{
		dial: dial,
	}
}

func (c *ReconnectingConsumer) Consume() <-chan Delivery {
	out := make(chan Delivery)
	go func() {
		for {
			consumer, err = c.dial()
			if err != nil {
				c.err = err
				return
			}
			c.consumer = consumer
			for d := range consumer.Consume() {
				out <- d
			}
			if err := consumer.Err(); err != nil {
				c.err = err
				return
			}
		}
	}()
	return out
}

func (c ReconnectingConsumer) Err() error {
	return c.err
}

func (c *ReconnectingConsumer) Stop() {
	if c.consumer != nil {
		c.consumer.Stop()
		c.consumer = nil
	}
}
