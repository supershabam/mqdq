package mqhammer

type Consumer interface {
	Consume() <-chan Delivery
	Err() error
	Stop()
}
