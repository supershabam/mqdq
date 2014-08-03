package mqhammer

import (
	"sync"
)

type MergeConsumer struct {
	Consumers []Consumer
	err       error
}

func (c MergeConsumer) Consume(done <-chan struct{}) (<-chan Delivery, error) {
	// wip

	go func() {

	}()
	return out, nil
}

func (c MergeConsumer) Err() error {
	return c.err
}
