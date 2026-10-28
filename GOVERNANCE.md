# Governance

event-order is a small library with one maintainer. This file says how decisions are made so contributors know what to expect.

## Maintainers

| Name | GitHub | Role |
|---|---|---|
| Aryan Deshmukh | [@A-RYAN-1](https://github.com/A-RYAN-1) | Maintainer: merges, releases, final say on scope |

Anyone can become a maintainer by sustained, good contributions (several merged pull requests and helpful reviews over time) and an invitation from the existing maintainers.

## How decisions are made

- **Scope.** The library does event-time ordering with a bounded delay, with the commit, duplicate and multi-source pieces around it. Proposals outside that go to a separate module or a different project. Open an issue first.
- **Pull requests** need a passing CI run, a DCO sign-off on every commit and the maintainer's approval. Every merge is a squash, so history stays linear.
- **API stability.** Until v1.0.0 a minor version may change the API; each change is in `CHANGELOG.md`. After v1.0.0, breaking changes need a major version.
- **Releases** are cut by the maintainer from `main` with a semantic version tag; only the maintainer can create tags.
- **Disagreement.** Discuss on the issue. If it does not settle, the maintainer decides and writes down why.

## Ownership and licence

The code is MIT licensed (`LICENSE`). Contributors keep the copyright on their contributions and license them under the same MIT terms; there is no copyright assignment and no CLA. The DCO sign-off (`git commit -s`) is the only formal step.

## Conduct and security

Behaviour is covered by `CODE_OF_CONDUCT.md`. Vulnerabilities go through `SECURITY.md`, never a public issue.
