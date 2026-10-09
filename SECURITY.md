# Security policy

Photon sits on the network edge of the services that use it, so security
reports get priority over everything else.

## Reporting a vulnerability

**Please do not open a public issue.** Report it privately through GitHub's
[private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
on this repository (the **Security** tab → **Report a vulnerability**).

Include what you can of:

- the version or commit,
- a description of the issue and its impact,
- a minimal program or request sequence that reproduces it.

## What to expect

- An acknowledgement within 3 working days.
- An assessment, and a fix or mitigation plan, within 14 days for a confirmed
  issue.
- Credit in the release notes and the advisory, unless you prefer otherwise.
- Coordinated disclosure: we agree a date with you, normally when a fixed
  release is available.

## Supported versions

Before 1.0, only the latest release receives security fixes.

## Scope

In scope: anything in this repository's root module — the router, server,
limits, streaming, error rendering, and `photonerr` — and the `photon` CLI.

Examples (`examples/`) and the benchmark harness (`bench/`) are demonstrations,
not supported software, though reports about them are still welcome.

Behaviour that is documented as the application's responsibility — for
example trusting `X-Forwarded-For`, or authentication — is out of scope unless
Photon's documentation or defaults mislead users about it. See
[docs/guides/security.md](docs/guides/security.md) for what Photon does and
does not protect against.
