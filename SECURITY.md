# Security policy

## Supported versions

Only the latest released version receives fixes.

## Reporting a vulnerability

Please do not open a public issue. Use GitHub's private vulnerability reporting (Security tab, "Report a vulnerability") on this repository. Expect an acknowledgement within 7 days. The library has no dependencies and does no I/O, so the likely reports are memory growth from crafted input (a missing cap) and panics on unusual times; both are in scope.
