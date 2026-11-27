# Security Policy

## Supported Versions

Only the latest release on the `main` branch receives security fixes.

| Version | Supported |
|---------|-----------|
| latest (`main`) | Yes |
| older releases | No |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.** Posting exploit details publicly puts all users at risk before a fix is available.

Instead, use GitHub's private reporting feature:

**[Report a vulnerability](https://github.com/quieromas-ai/code-dioxide/security/advisories/new)**

Please include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept (if safe to share)
- Affected versions or configurations

**Response expectations:**
- Acknowledgement within 48 hours
- Triage and initial assessment within 7 days
- Fix and coordinated disclosure timeline communicated as soon as possible

## Scope

**In scope:**
- Authentication or authorization bypass in the MCP server, REST API or Reporting API (admin, reports and content tokens)
- Token or credential leakage through logs, error responses, API endpoints or the child session environment
- Redaction bypass: secrets reaching a reports or content response unmasked
- Remote code execution via the server process
- SQL injection or data exposure through the task database

**Out of scope:**
- What the Claude Code sessions dioxide starts do with the tools and permissions you grant them
- The documented limits in the README (Security, Redaction and its limits, Process kill limits), unless the code does not meet them
- Exposure caused by binding the server to a non-loopback address without a reverse proxy and TLS
- Vulnerabilities in user-managed infrastructure (OS, launchd, reverse proxy)
- Issues in upstream dependencies (report those to the respective project)
- Security issues in third-party services (Anthropic API, Claude Code CLI)

## Disclosure Policy

We follow coordinated disclosure. A fix will be prepared and released before vulnerability details are made public. We will credit reporters in the release notes unless anonymity is requested.
