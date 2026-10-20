# Security Testing Guide

**NIS2 Compliance**: Article 21.2(a) - Risk Analysis and Information System Security Policies

This document describes the security testing framework for the Unified OT Zerobus Connector Client, designed to meet NIS2 cybersecurity requirements.

---

## Overview

The security testing framework includes:

1. **Automated Security Tests** - pytest-based test suite
2. **Security Headers** - Protection against common web vulnerabilities
3. **Input Validation** - Prevention of injection attacks
4. **Vulnerability Scanning** - OWASP ZAP integration
5. **Security Audit Logging** - Comprehensive event tracking

---

## Security Controls Implemented

### 1. Security Headers

**Purpose**: Protect against XSS, clickjacking, MIME sniffing, and information leakage.

**Headers Implemented**:
- `Content-Security-Policy` - Restricts resource loading
- `X-Frame-Options: DENY` - Prevents clickjacking
- `X-Content-Type-Options: nosniff` - Prevents MIME sniffing
- `X-XSS-Protection: 1; mode=block` - Enables XSS filter
- `Referrer-Policy` - Limits information leakage
- `Permissions-Policy` - Disables unnecessary browser features
- `Cache-Control` - Prevents caching of sensitive data
- `Strict-Transport-Security` (HSTS) - Forces HTTPS (production only)

**Test Coverage**:
```bash
pytest tests/security/test_security_headers.py -v
```

### 2. Input Validation

**Purpose**: Prevent injection attacks (command injection, path traversal, XSS).

**Validations Implemented**:
- **Source Names**: Alphanumeric, hyphens, underscores, periods only
- **Protocols**: Whitelist (opcua, mqtt, modbus, sparkplug)
- **Endpoints**: URL format validation, dangerous character blocking
- **Hostnames/IPs**: Format validation, length limits
- **Ports**: Range validation (1-65535)
- **Configuration Values**: Type validation, length limits, null byte checking

**Attack Vectors Blocked**:
- Command injection: `; rm -rf /`, `| cat /etc/passwd`, `` `whoami` ``
- Path traversal: `../../../etc/passwd`, `..\\windows\\system32`
- Variable expansion: `${USER}`, `$(command)`
- XSS: `<script>`, `javascript:`
- File protocol: `file:///etc/passwd`
- Null bytes: `\x00`

**Test Coverage**:
```bash
pytest tests/security/test_input_validation.py -v
```

### 3. Authentication & Authorization

**Purpose**: NIS2 Article 21.2(g) - Access Control

**Security Features**:
- OAuth2 with OpenID Connect
- MFA verification
- Role-based access control (3 roles, 6 permissions)
- Encrypted session management
- Session timeout (8 hours)
- Audit logging

**Test Coverage**:
```bash
pytest tests/security/test_authentication.py -v
pytest tests/security/test_rbac.py -v
```

---

## Running Security Tests

### 1. Run All Security Tests

```bash
# From repository root
pytest tests/security/ -v

# With coverage
pytest tests/security/ --cov=unified_connector.web --cov-report=html
```

### 2. Run Specific Test Categories

```bash
# Security headers only
pytest tests/security/test_security_headers.py -v

# Input validation only
pytest tests/security/test_input_validation.py -v

# Authentication only
pytest tests/security/test_authentication.py -v
```

### 3. Generate Security Test Report

```bash
pytest tests/security/ --html=security_test_report.html --self-contained-html
```

---

## Manual Security Testing

### 1. Test Security Headers

```bash
# Test CSP header
curl -I http://localhost:8082/ | grep "Content-Security-Policy"

# Test X-Frame-Options
curl -I http://localhost:8082/ | grep "X-Frame-Options"

# Test all security headers
curl -I http://localhost:8082/ | grep -E "(Content-Security-Policy|X-Frame-Options|X-Content-Type-Options|X-XSS-Protection|Referrer-Policy|Permissions-Policy)"
```

### 2. Test Input Validation

```bash
# Test path traversal prevention
curl -X POST http://localhost:8082/api/sources \
  -H "Content-Type: application/json" \
  -d '{"name": "../../../etc/passwd", "protocol": "opcua", "endpoint": "opc.tcp://host:4840"}'
# Expected: 400 Bad Request with validation error

# Test command injection prevention
curl -X POST http://localhost:8082/api/sources \
  -H "Content-Type: application/json" \
  -d '{"name": "test", "protocol": "opcua", "endpoint": "opc.tcp://host;rm -rf /"}'
# Expected: 400 Bad Request with validation error

# Test XSS prevention
curl -X POST http://localhost:8082/api/sources \
  -H "Content-Type: application/json" \
  -d '{"name": "<script>alert(1)</script>", "protocol": "opcua", "endpoint": "opc.tcp://host:4840"}'
# Expected: 400 Bad Request with validation error
```

### 3. Test Authentication

```bash
# Test unauthenticated access blocked
curl -I http://localhost:8082/api/sources
# Expected: 401 Unauthorized or redirect to login

# Test authenticated access
curl -H "Cookie: unified_connector_session=<session_cookie>" http://localhost:8082/api/sources
# Expected: 200 OK with sources list

# Test session expiration
# Wait 8+ hours, then try to access API
# Expected: 401 Unauthorized
```

### 4. Test RBAC

```bash
# Login as viewer
# Try to delete source (should fail)
curl -X DELETE -H "Cookie: session=..." http://localhost:8082/api/sources/test
# Expected: 403 Forbidden

# Login as admin
# Try to delete source (should succeed)
curl -X DELETE -H "Cookie: session=..." http://localhost:8082/api/sources/test
# Expected: 200 OK
```

---

## Vulnerability Scanning with OWASP ZAP

### 1. Install OWASP ZAP

```bash
# macOS
brew install --cask owasp-zap

# Ubuntu/Debian
sudo apt install zaproxy

# Or download from https://www.zaproxy.org/download/
```

### 2. Run Automated Scan

```bash
# Start connector
docker-compose -f docker-compose.unified.yml up -d

# Run ZAP baseline scan
docker run -t owasp/zap2docker-stable zap-baseline.py \
  -t http://host.docker.internal:8082 \
  -r zap_report.html

# View report
open zap_report.html
```

### 3. Run Full Scan

```bash
# Run ZAP full scan (takes longer, more thorough)
docker run -t owasp/zap2docker-stable zap-full-scan.py \
  -t http://host.docker.internal:8082 \
  -r zap_full_report.html

# View report
open zap_full_report.html
```

### 4. Expected ZAP Results

**Should PASS (No Alerts)**:
- ✅ No SQL injection vulnerabilities (we don't use SQL)
- ✅ No command injection vulnerabilities (input validation blocks)
- ✅ No path traversal vulnerabilities (validation blocks `..`)
- ✅ Security headers present (CSP, X-Frame-Options, etc.)
- ✅ No XSS vulnerabilities (CSP + input validation)

**May Report (False Positives)**:
- ⚠️ CSP header includes 'unsafe-inline' (required for embedded scripts/styles)
- ⚠️ HSTS not enabled (only enable with HTTPS in production)

---

## Security Checklist

Before deploying to production, verify:

- [ ] All security tests pass (`pytest tests/security/ -v`)
- [ ] OWASP ZAP scan shows no critical/high vulnerabilities
- [ ] Authentication is enabled in config.yaml
- [ ] MFA is required (require_mfa: true)
- [ ] SESSION_SECRET_KEY is set to strong random value
- [ ] OAuth credentials are configured
- [ ] HTTPS is enabled (set security headers HSTS)
- [ ] Environment variables are set (.env file not committed)
- [ ] Credentials are encrypted (CONNECTOR_MASTER_PASSWORD set)
- [ ] Logs are being shipped to SIEM (Sprint 1.3)
- [ ] Security monitoring is active
- [ ] Incident response plan is documented

---

## Common Vulnerabilities Tested

### 1. Injection Attacks

**Types Tested**:
- SQL injection (N/A - no SQL database)
- Command injection (`; rm -rf /`, `| cat /etc/passwd`)
- LDAP injection (N/A - no LDAP)
- OS command injection in endpoints
- Variable expansion (`${USER}`, `$(command)`)

**Test Method**:
```python
def test_command_injection():
    config = {
        'name': 'test; rm -rf /',
        'protocol': 'opcua',
        'endpoint': 'opc.tcp://host:4840'
    }
    with pytest.raises(ValidationError):
        validate_source_config(config)
```

### 2. Cross-Site Scripting (XSS)

**Protection Layers**:
1. Content-Security-Policy header
2. Input validation (reject `<`, `>`, `"`, `'`)
3. Output escaping in JavaScript (escapeHtml function)

**Test Method**:
```python
def test_xss_prevention():
    name = "<script>alert('XSS')</script>"
    with pytest.raises(ValidationError):
        validate_source_name(name)
```

### 3. Path Traversal

**Attack Examples Blocked**:
- `../../../etc/passwd`
- `..\\windows\\system32`
- `test/../secret`

**Test Method**:
```python
def test_path_traversal():
    name = '../../../etc/passwd'
    with pytest.raises(ValidationError, match="path traversal"):
        validate_source_name(name)
```

### 4. Clickjacking

**Protection**: `X-Frame-Options: DENY` header

**Test Method**:
```bash
curl -I http://localhost:8082/ | grep "X-Frame-Options: DENY"
```

### 5. Session Hijacking

**Protection Measures**:
- Encrypted session cookies (Fernet)
- HttpOnly flag (JavaScript cannot access)
- SameSite=Lax (CSRF protection)
- Session timeout (8 hours)
- Secure flag in production (HTTPS only)

---

## Security Monitoring

### 1. Security Event Logging

All security events are logged with structured data:

```python
logger.warning(
    f"Source validation failed: {error}",
    extra={
        'event_type': 'security',
        'action': 'add_source',
        'result': 'validation_failed',
        'user': user.email,
        'source_ip': request.remote,
        'timestamp': datetime.utcnow().isoformat()
    }
)
```

### 2. SIEM Integration (Sprint 1.3)

Security logs will be shipped to SIEM for:
- Real-time alerting on suspicious activity
- Correlation with other security events
- Compliance reporting
- Incident investigation

---

## Reporting Security Issues

If you discover a security vulnerability:

1. **DO NOT** create a public GitHub issue
2. Email security contact: [security email]
3. Provide:
   - Vulnerability description
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if any)

---

## References

- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [OWASP Testing Guide](https://owasp.org/www-project-web-security-testing-guide/)
- [NIS2 Directive (EU) 2022/2555](https://eur-lex.europa.eu/eli/dir/2022/2555)
- [CIS Critical Security Controls](https://www.cisecurity.org/controls)

---

**Last Updated**: 2025-01-31
**NIS2 Compliance**: Phase 1, Sprint 1.2 - Security Testing Framework
