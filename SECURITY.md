# Security Policy

Security-response owner: Backblaze Security. Repository maintainers triage reports with them.

## Supported Versions

The latest release of b2sdk (currently `2.13.x`) receives security fixes. Every apiver layer
(`b2sdk.v0`..`b2sdk.v3`) inside that release is covered. Earlier releases are not patched in place;
upgrade to the newest release to receive fixes. See [`RELEASE.md`](RELEASE.md).

| Version | Supported |
|---|---|
| Latest release (`2.13.x`) | Yes |
| Older releases | No |

## Reporting a Vulnerability

**Do not file a public GitHub issue for a security vulnerability.**

Instead, report it privately via GitHub Security Advisories:

1. Go to the [Security tab](https://github.com/Backblaze/b2-sdk-python/security/advisories/new) of this repository
2. Click **Report a vulnerability**
3. Provide a clear description, the affected version, reproduction steps, and impact assessment

You can also email **security@backblaze.com** if you cannot use GitHub Security Advisories.

## What to expect

- Acknowledgement within 3 business days
- Initial assessment within 7 business days
- Coordinated disclosure once a fix is available, shipped as a patch release with a changelog entry and a
  GitHub security advisory

## Scope

In scope:

- Credential-handling flaws in `b2sdk`: application keys, authorization tokens or SSE-C keys exposed in
  logs, exception messages, `repr`, or sent to unintended hosts
- Account-info storage: `SqliteAccountInfo` files created readable by other users, or credentials cached
  where the caller did not ask
- Transport flaws: TLS verification bypassed, or requests sent outside the key's bucket or prefix scope
- Sync and download path handling that writes outside the destination folder
- Denial of service reachable through the public API, such as unbounded retries or thread leaks triggered
  by server responses

Out of scope:

- Vulnerabilities in the Backblaze B2 service itself — report those to Backblaze directly
- Misuse of credentials by the calling application, such as embedding a master key in client code
- Issues requiring root or local access on the machine running the application

## Security-relevant areas

- **Credentials**: application keys go to `B2Api.authorize_account` and only to the B2 authorize
  endpoint; the token returned is kept in the account-info store and sent in the `Authorization` header.
- **Account info**: `SqliteAccountInfo` creates its database with owner-only permissions before writing
  credentials to it.
- **SSE-C keys**: customer-provided keys travel in request headers on uploads and downloads, and as
  `customerKey` fields in JSON request bodies on large-file start (`serverSideEncryption`) and server-side
  copy (`sourceServerSideEncryption`, `destinationServerSideEncryption`). They are not persisted. If you
  log requests, redact encryption keys in both headers and bodies.
- **Transport**: all B2 API traffic uses HTTPS through `requests`, with certificates from `certifi`.
- **Tests and CI**: integration tests read `B2_TEST_APPLICATION_KEY_ID` and `B2_TEST_APPLICATION_KEY` from
  repository secrets. The `.sdkharness` checks refuse any non-loopback endpoint and ambient `B2_*`
  credentials.

## Best practices for users

- Use application keys scoped to specific buckets, prefixes and capabilities.
- Never embed an application key in code shipped to end users; issue download authorizations from a
  backend instead.
- Rotate application keys regularly.
- Use SSE-B2 or SSE-C for encryption at rest, and Object Lock for compliance-sensitive data.
