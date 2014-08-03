package mqhammer

type Consumer interface {
	Consume(<-chan struct{}) (<-chan Delivery, error)
	Err() error
}
