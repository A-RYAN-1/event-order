# Contributing

Thanks for looking. The library is deliberately small: event-time ordering with a bounded delay, nothing else.

1. **Open an issue first** for anything beyond a typo or an obvious bug, so we agree it belongs before you spend time.
2. **Tests are required.** A bug fix starts with a test that fails; a feature comes with tests and an example. Run `go vet ./... && go test -race ./...` and `gofmt -l .` (it must print nothing).
3. **Keep the API stable and the module dependency-free.** New options are fine; new dependencies are not.
4. **Commit style:** one imperative line in the Conventional Commits form (`fix: release equal times in arrival order`); the pull request title uses the same form and a bot checks it. Body if the reason is not obvious.
5. **Sign off every commit** with `git commit -s` (the Developer Certificate of Origin, https://developercertificate.org). It states you wrote the change or may submit it; you keep your copyright and nothing is assigned. A check fails the pull request when a sign-off is missing; `git rebase --signoff origin/main` fixes it.
6. **Review:** every change goes through a pull request approved by the maintainer; CI (`go vet`, `gofmt`, `go test -race` on Go 1.23 and stable, lint, vulnerability scan) must pass.

By submitting a pull request you agree that your contribution is licensed under the MIT License in `LICENSE`, the same as the rest of the project.

Performance changes need a before and after: run `scripts/bench.sh before.txt` on the base commit and `scripts/bench.sh after.txt` on yours, then show `benchstat before.txt after.txt` (needs `go install golang.org/x/perf/cmd/benchstat@latest`).
