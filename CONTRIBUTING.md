# Contributing

Thanks for looking. The library is deliberately small: event-time ordering with a bounded delay, nothing else.

1. **Open an issue first** for anything beyond a typo or an obvious bug, so we agree it belongs before you spend time.
2. **Tests are required.** A bug fix starts with a test that fails; a feature comes with tests and an example. Run `go vet ./... && go test -race ./...` and `gofmt -l .` (it must print nothing).
3. **Keep the API stable and the module dependency-free.** New options are fine; new dependencies are not.
4. **Commit style:** one imperative line (`fix: release equal times in arrival order`), body if the reason is not obvious.
5. **Review:** every change goes through a pull request approved by the maintainer; CI (`go vet`, `gofmt`, `go test -race` on Go 1.23 and stable) must pass.

Performance changes need a before and after from `go test -run xxx -bench . -benchmem`.
