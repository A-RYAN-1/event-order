module github.com/A-RYAN-1/event-order/examples/redisstreams

go 1.26.0

replace github.com/A-RYAN-1/event-order => ../..

require (
	github.com/A-RYAN-1/event-order v0.0.0-00010101000000-000000000000
	github.com/redis/go-redis/v9 v9.23.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)
