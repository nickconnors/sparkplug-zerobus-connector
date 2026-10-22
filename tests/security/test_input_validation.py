"""
Input Validation Tests

Tests input validation to prevent injection attacks.
"""

import pytest
from unified_connector.web.input_validation import (
    validate_source_name,
    validate_protocol,
    validate_endpoint,
    validate_host,
    validate_port,
    validate_source_config,
    ValidationError
)


class TestSourceNameValidation:
    """Test source name validation."""

    def test_valid_names(self):
        """Test valid source names are accepted."""
        valid_names = [
            'my-source',
            'source_1',
            'Source.123',
            'OPC-UA-Server',
            'mqtt_broker_1',
        ]
        for name in valid_names:
            assert validate_source_name(name) == name

    def test_empty_name(self):
        """Test empty name is rejected."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_source_name('')

    def test_path_traversal(self):
        """Test path traversal is rejected."""
        malicious_names = [
            '../etc/passwd',
            '..\\windows\\system32',
            'test/../secret',
            'test/../../root',
        ]
        for name in malicious_names:
            with pytest.raises(ValidationError, match="path traversal"):
                validate_source_name(name)

    def test_special_characters(self):
        """Test special characters are rejected."""
        malicious_names = [
            'test;rm -rf /',
            'test|cat /etc/passwd',
            'test`whoami`',
            'test$USER',
            'test<script>',
        ]
        for name in malicious_names:
            with pytest.raises(ValidationError):
                validate_source_name(name)

    def test_too_long(self):
        """Test overly long names are rejected."""
        long_name = 'a' * 300
        with pytest.raises(ValidationError, match="too long"):
            validate_source_name(long_name)


class TestProtocolValidation:
    """Test protocol validation."""

    def test_valid_protocols(self):
        """Test valid protocols are accepted."""
        for protocol in ('sparkplug', 'SPARKPLUG', ' Sparkplug '):
            assert validate_protocol(protocol) == 'sparkplug'

    def test_invalid_protocol(self):
        """Test invalid protocols are rejected."""
        for protocol in ('http', 'opcua', 'mqtt', 'modbus'):
            with pytest.raises(ValidationError, match="Invalid protocol"):
                validate_protocol(protocol)

        with pytest.raises(ValidationError, match="Invalid protocol"):
            validate_protocol('malicious')


class TestEndpointValidation:
    """Test endpoint URL validation."""

    def test_valid_endpoints(self):
        """Test valid endpoints are accepted."""
        valid_endpoints = [
            'mqtt://localhost:1883',
            'mqtts://broker.example.com:8883',
            'mqtt://10.0.0.50:1883',
            'https://example.com:443/api',
        ]
        for endpoint in valid_endpoints:
            assert validate_endpoint(endpoint) == endpoint

    def test_empty_endpoint(self):
        """Test empty endpoint is rejected."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_endpoint('')

    def test_command_injection(self):
        """Test command injection attempts are rejected."""
        malicious_endpoints = [
            'mqtt://host;rm -rf /',
            'mqtt://host|cat /etc/passwd',
            'mqtts://host`whoami`',
            'http://host$(uname -a)',
        ]
        for endpoint in malicious_endpoints:
            with pytest.raises(ValidationError):
                validate_endpoint(endpoint)

    def test_variable_expansion(self):
        """Test variable expansion is rejected."""
        malicious_endpoints = [
            'http://${USER}:${PASS}@host',
            'mqtt://${HOSTNAME}',
        ]
        for endpoint in malicious_endpoints:
            with pytest.raises(ValidationError):
                validate_endpoint(endpoint)

    def test_path_traversal_in_endpoint(self):
        """Test path traversal in endpoint is rejected."""
        with pytest.raises(ValidationError):
            validate_endpoint('http://host/../../../etc/passwd')

    def test_file_protocol(self):
        """Test file:// protocol is rejected."""
        with pytest.raises(ValidationError):
            validate_endpoint('file:///etc/passwd')

    def test_javascript_protocol(self):
        """Test javascript: protocol is rejected (XSS)."""
        with pytest.raises(ValidationError):
            validate_endpoint('javascript:alert(1)')

    def test_too_long(self):
        """Test overly long endpoints are rejected."""
        long_endpoint = 'http://example.com/' + 'a' * 2000
        with pytest.raises(ValidationError, match="too long"):
            validate_endpoint(long_endpoint)


class TestHostValidation:
    """Test hostname/IP validation."""

    def test_valid_ips(self):
        """Test valid IP addresses are accepted."""
        valid_ips = ['192.168.1.1', '10.0.0.1', '172.16.0.1', '127.0.0.1']
        for ip in valid_ips:
            assert validate_host(ip) == ip

    def test_valid_hostnames(self):
        """Test valid hostnames are accepted."""
        valid_hosts = ['localhost', 'example.com', 'sub.example.com', 'my-host']
        for host in valid_hosts:
            assert validate_host(host) == host

    def test_invalid_ip(self):
        """Test invalid IPs are rejected."""
        invalid_ips = ['256.1.1.1', '1.1.1.256', '999.999.999.999']
        for ip in invalid_ips:
            with pytest.raises(ValidationError):
                validate_host(ip)

    def test_empty_host(self):
        """Test empty host is rejected."""
        with pytest.raises(ValidationError, match="cannot be empty"):
            validate_host('')

    def test_too_long_hostname(self):
        """Test overly long hostnames are rejected."""
        long_host = 'a' * 300 + '.com'
        with pytest.raises(ValidationError, match="too long"):
            validate_host(long_host)


class TestPortValidation:
    """Test port number validation."""

    def test_valid_ports(self):
        """Test valid ports are accepted."""
        valid_ports = [1, 80, 443, 4840, 1883, 502, 65535]
        for port in valid_ports:
            assert validate_port(port) == port
            assert validate_port(str(port)) == port

    def test_invalid_ports(self):
        """Test invalid ports are rejected."""
        invalid_ports = [0, -1, 65536, 100000]
        for port in invalid_ports:
            with pytest.raises(ValidationError, match="between 1 and 65535"):
                validate_port(port)

    def test_non_numeric_port(self):
        """Test non-numeric ports are rejected."""
        with pytest.raises(ValidationError, match="must be a number"):
            validate_port('abc')


class TestSourceConfigValidation:
    """Test complete source configuration validation."""

    def test_valid_config(self):
        """Test valid configuration is accepted."""
        config = {
            'name': 'test-source',
            'protocol': 'sparkplug',
            'endpoint': 'mqtts://192.168.1.100:8883',
            'enabled': True,
            'primary_host_id': 'databricks-zerobus',
            'site': 'plant1',                       # not a Sparkplug setting: dropped
        }
        validated = validate_source_config(config)
        assert validated['name'] == 'test-source'
        assert validated['protocol'] == 'sparkplug'
        assert validated['enabled'] is True and validated['primary_host_id'] == 'databricks-zerobus'
        assert 'site' not in validated

    def test_missing_required_fields(self):
        """Test missing required fields are rejected."""
        incomplete_configs = [
            {'protocol': 'sparkplug', 'endpoint': 'mqtt://host:1883'},  # missing name
            {'name': 'test', 'endpoint': 'mqtt://host:1883'},  # missing protocol
            {'name': 'test', 'protocol': 'sparkplug'},  # missing endpoint
        ]
        for config in incomplete_configs:
            with pytest.raises(ValidationError, match="Missing required field"):
                validate_source_config(config)

    def test_malicious_config(self):
        """Test malicious configuration is rejected."""
        malicious_config = {
            'name': '../../../etc/passwd',
            'protocol': 'sparkplug',
            'endpoint': 'mqtt://host;rm -rf /',
        }
        with pytest.raises(ValidationError):
            validate_source_config(malicious_config)
