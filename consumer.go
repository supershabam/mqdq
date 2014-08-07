package mqhammer

type NewConsumer func() (Consumer, error)

type Consumer interface {
	Consume() <-chan Delivery
	Err() error
	Stop()
}
