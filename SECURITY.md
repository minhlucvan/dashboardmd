# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability in dashboardmd, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

Instead, please report vulnerabilities by emailing the maintainers or using
[GitHub's private vulnerability reporting](https://github.com/minhlucvan/dashboardmd/security/advisories/new).

### What to Include

- Description of the vulnerability
- Steps to reproduce the issue
- Potential impact
- Suggested fix (if any)

### Response Timeline

- **Acknowledgment:** Within 48 hours
- **Initial assessment:** Within 1 week
- **Fix and release:** Dependent on severity, typically within 2 weeks for critical issues

## Security Considerations

dashboardmd generates Markdown dashboards from Python code and data sources. Key security notes:

- **File paths:** The library writes to user-specified file paths. Ensure output paths are trusted.
- **Data sources:** The library reads from user-specified data sources (CSV, databases). Ensure sources are trusted.
- **Interop credentials:** BI platform connectors (Metabase, Looker, etc.) handle API keys. Never commit credentials to source control.
- **User input in reports:** If your dashboards include user-provided data, be mindful of Markdown injection.
