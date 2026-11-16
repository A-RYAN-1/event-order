module github.com/A-RYAN-1/event-order/examples/nats

go 1.26.0

replace github.com/A-RYAN-1/event-order => ../..

require (
	github.com/A-RYAN-1/event-order v0.0.0-00010101000000-000000000000
	github.com/nats-io/nats.go v1.55.0
)

require (
	github.com/klauspost/compress v1.20.0 // indirect
	github.com/nats-io/nkeys v0.4.16 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)
