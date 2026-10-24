module github.com/A-RYAN-1/event-order/examples/kafka

go 1.23

replace github.com/A-RYAN-1/event-order => ../..

require (
	github.com/A-RYAN-1/event-order v0.0.0-00010101000000-000000000000
	github.com/segmentio/kafka-go v0.4.51
)

require (
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
)
