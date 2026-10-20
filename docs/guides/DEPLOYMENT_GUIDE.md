# Unified Connector - Deployment Guide

Complete guide for deploying the Unified OT Zerobus Connector Client in local, Docker, and production environments.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Local Development Setup](#local-development-setup)
3. [Docker/Colima Deployment](#dockercolima-deployment)
4. [Testing with OT Simulator](#testing-with-ot-simulator)
5. [Production Deployment](#production-deployment)
6. [Security Configuration](#security-configuration)
7. [Troubleshooting](#troubleshooting)

## Prerequisites

### Software Requirements

- **Python**: 3.11 or higher
- **Docker**: 24.0 or higher (for container deployment)
- **Colima**: 0.6.0 or higher (macOS/Linux alternative to Docker Desktop)
- **Docker Compose**: 2.20 or higher
- **Git**: For version control

### Databricks Requirements

- Databricks workspace (Azure, AWS, or GCP)
- ZeroBus enabled on workspace
- OAuth2 M2M credentials (client_id + client_secret)
- Delta table created for data ingestion

### Network Requirements

- Access to OPC-UA servers (port 4840)
- Access to MQTT brokers (ports 1883, 8883), including the MQTT server of Sparkplug B edges (e.g. Chariot)
- Access to Modbus servers (ports 502, 5020)
- Outbound HTTPS to Databricks workspace

## Local Development Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd opc-ua-zerobus-connector
```

### 2. Create Virtual Environment

```bash
cd unified_connector
python3.11 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Credentials

Generate master password for encryption:

```bash
python -m unified_connector.core.credential_manager generate-password
```

Output:
```
Generated master password: <random-base64-string>

Set this as environment variable:
export CONNECTOR_MASTER_PASSWORD='<random-base64-string>'
```

Set environment variable:

```bash
export CONNECTOR_MASTER_PASSWORD='<generated-password>'
```

Store Databricks credentials:

```bash
python -m unified_connector.core.credential_manager store zerobus.client_id "<your-client-id>"
python -m unified_connector.core.credential_manager store zerobus.client_secret "<your-client-secret>"
```

Verify:

```bash
python -m unified_connector.core.credential_manager list
```

### 5. Configure Application

Edit `unified_connector/config/config.yaml`:

```yaml
# Minimal config for local dev
connector:
  name: "Unified OT Zerobus Connector Client"
  log_level: "DEBUG"

web_ui:
  enabled: true
  host: "0.0.0.0"
  port: 8080

discovery:
  enabled: true
  network:
    scan_enabled: true
    subnets:
      - "192.168.1.0/24"  # Your local network
      - "172.17.0.0/24"   # Docker network

zerobus:
  enabled: false  # Start disabled, enable via Web UI
  default_target:
    workspace_host: "https://adb-xxx.azuredatabricks.net"
    catalog: "main"
    schema: "iot_data"
    table: "sensor_readings"
  auth:
    client_id: "${credential:zerobus.client_id}"
    client_secret: "${credential:zerobus.client_secret}"

normalization:
  enabled: true
```

### 6. Run Locally

```bash
python -m unified_connector
```

Output:
```
================================================================================
Unified OT Zerobus Connector Client - Starting
================================================================================
✓ Discovery service started
✓ Bridge started
✓ Web UI started
================================================================================
✓ Unified Connector is running
================================================================================
Web UI: http://0.0.0.0:8080
Press Ctrl+C to stop
```

### 7. Access Web UI

Open browser: http://localhost:8080

## Docker/Colima Deployment

### 1. Install Colima (macOS/Linux)

```bash
# macOS
brew install colima

# Linux (see https://github.com/abiosoft/colima)
```

### 2. Start Colima

```bash
# Start with 4 CPU, 8GB RAM, 50GB disk
colima start --cpu 4 --memory 8 --disk 50

# Verify
colima status
```

Output:
```
INFO[0000] colima is running
INFO[0000] arch: aarch64
INFO[0000] runtime: docker
INFO[0000] mountType: sshfs
INFO[0000] socket: unix:///Users/<user>/.colima/default/docker.sock
```

### 3. Configure Docker Context

```bash
# Set Docker to use Colima
docker context use colima

# Verify
docker ps
```

### 4. Build Images

```bash
cd opc-ua-zerobus-connector
docker-compose -f docker-compose.unified.yml build
```

### 5. Configure Environment

Create `.env` file:

```bash
cat > .env << 'EOF'
# Master password for credential encryption
CONNECTOR_MASTER_PASSWORD=<your-generated-password>

# Optional: Set via Web UI instead
CONNECTOR_ZEROBUS_CLIENT_ID=<databricks-client-id>
CONNECTOR_ZEROBUS_CLIENT_SECRET=<databricks-client-secret>

# Logging
CONNECTOR_LOG_LEVEL=INFO
EOF
```

### 6. Start Services

```bash
docker-compose -f docker-compose.unified.yml up -d
```

### 7. Verify Services

```bash
# Check containers
docker-compose -f docker-compose.unified.yml ps

# Check logs
docker-compose -f docker-compose.unified.yml logs -f

# Check health
curl http://localhost:8081/health
curl http://localhost:8989/health  # OT Simulator
```

### 8. Stop Services

```bash
docker-compose -f docker-compose.unified.yml down
```

To also remove volumes (WARNING: deletes data):

```bash
docker-compose -f docker-compose.unified.yml down -v
```

## Testing with OT Simulator

The project includes an OT Simulator for generating test data.

### 1. Start Services

```bash
# Start OT Simulator and Unified Connector
docker-compose -f docker-compose.unified.yml up -d

# Wait for services to start
sleep 10
```

### 2. Run Automated Tests

```bash
# Run test script
./test_unified_connector.sh
```

The script will:
- ✓ Check prerequisites
- ✓ Build Docker images
- ✓ Start OT Simulator
- ✓ Start Unified Connector
- ✓ Test protocol discovery
- ✓ Check metrics and status
- ✓ Display logs

### 3. Manual Testing

**Verify OT Simulator:**

```bash
# Check simulator web UI
open http://localhost:8989

# Check OPC-UA server
docker exec -it ot-simulator python -c "
from asyncua import Client
import asyncio

async def test():
    client = Client('opc.tcp://localhost:4840/ot-simulator/server/')
    await client.connect()
    print('✓ OPC-UA server reachable')
    await client.disconnect()

asyncio.run(test())
"
```

**Test Protocol Discovery:**

```bash
# Trigger discovery
curl -X POST http://localhost:8080/api/discovery/scan

# Wait 5 seconds
sleep 5

# Check discovered servers
curl http://localhost:8080/api/discovery/servers | jq
```

Expected output:
```json
{
  "servers": [
    {
      "protocol": "opcua",
      "host": "172.20.0.2",
      "port": 4840,
      "endpoint": "opc.tcp://172.20.0.2:4840",
      "name": "OT Data Simulator - OPC UA",
      "reachable": true
    },
    {
      "protocol": "mqtt",
      "host": "172.20.0.2",
      "port": 1883,
      "endpoint": "mqtt://172.20.0.2:1883",
      "name": "MQTT Broker (172.20.0.2)",
      "reachable": true
    }
  ],
  "count": 2
}
```

**Add OPC-UA Source:**

```bash
curl -X POST http://localhost:8080/api/sources \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ot_simulator_opcua",
    "protocol": "opcua",
    "endpoint": "opc.tcp://ot-simulator:4840",
    "enabled": true,
    "polling_mode": true,
    "polling_interval_ms": 1000,
    "site": "test_plant",
    "area": "production",
    "line": "line1",
    "equipment": "simulator"
  }'
```

**Monitor Data Flow:**

```bash
# Check metrics every 5 seconds
watch -n 5 'curl -s http://localhost:8080/api/metrics | jq ".bridge"'
```

Expected metrics:
```json
{
  "records_received": 12543,
  "records_normalized": 12543,
  "records_enqueued": 12543,
  "records_dropped": 0,
  "batches_sent": 13,
  "reconnections": 0
}
```

## Production Deployment

### 1. Security Hardening

**Generate Strong Master Password:**

```bash
python -m unified_connector.core.credential_manager generate-password > master_password.txt
chmod 600 master_password.txt
```

**Store Credentials in Secret Manager:**

Instead of environment variables, use:
- AWS Secrets Manager
- Azure Key Vault
- GCP Secret Manager
- HashiCorp Vault

**Enable TLS for Protocol Connections:**

```yaml
# OPC-UA with security
sources:
  - name: "production_opcua"
    protocol: "opcua"
    endpoint: "opc.tcp://prod-server:4840"
    security_mode: "SignAndEncrypt"
    security_policy: "Basic256Sha256"
    username: "${credential:opcua.username}"
    password: "${credential:opcua.password}"

# MQTT with TLS
sources:
  - name: "production_mqtt"
    protocol: "mqtt"
    endpoint: "mqtt://prod-broker:8883"
    use_tls: true
    ca_cert: "/certs/ca.pem"
    client_cert: "/certs/client.pem"
    client_key: "/certs/client-key.pem"
```

### 2. Resource Configuration

**Adjust docker-compose for production:**

```yaml
services:
  unified_connector:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 8G
        reservations:
          cpus: '2'
          memory: 4G

    environment:
      - CONNECTOR_LOG_LEVEL=INFO
      - CONNECTOR_MASTER_PASSWORD_FILE=/run/secrets/master_password

    secrets:
      - master_password

    restart: always

secrets:
  master_password:
    external: true
```

### 3. Monitoring Setup

**Prometheus Integration:**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'unified_connector'
    static_configs:
      - targets: ['unified_connector:9090']
```

**Key Metrics to Monitor:**

- `connector_records_received_total` - Total records ingested
- `connector_records_dropped_total` - Records lost due to backpressure
- `connector_batches_sent_total` - Batches sent to ZeroBus
- `connector_reconnections_total` - Connection failures
- `connector_backpressure_queue_size` - Current queue depth

### 4. Backup Configuration

**Regular Config Backups:**

```bash
# Backup script
#!/bin/bash
BACKUP_DIR="/backups/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

# Backup config
cp unified_connector/config/config.yaml "$BACKUP_DIR/"

# Backup credentials (encrypted)
cp ~/.unified_connector/credentials.enc "$BACKUP_DIR/"
cp ~/.unified_connector/salt.txt "$BACKUP_DIR/"

echo "Backup complete: $BACKUP_DIR"
```

### 5. High Availability

**Run Multiple Instances:**

```yaml
services:
  unified_connector:
    deploy:
      replicas: 3
      update_config:
        parallelism: 1
        delay: 10s
```

**Load Balancing:**

Use HAProxy or nginx to distribute sources across instances.

## Security Configuration

### Credential Management

**Best Practices:**

1. **Never commit credentials to git**
2. **Use environment variables or secret managers**
3. **Rotate credentials regularly (90 days)**
4. **Use separate credentials per environment**
5. **Audit credential access**

**Credential Rotation:**

```bash
# Generate new credentials in Databricks
# Update connector
python -m unified_connector.core.credential_manager store zerobus.client_id "<new-client-id>"
python -m unified_connector.core.credential_manager store zerobus.client_secret "<new-client-secret>"

# Restart connector (rolling restart in production)
docker-compose -f docker-compose.unified.yml restart unified_connector
```

### Network Security

**Firewall Rules:**

```bash
# Allow outbound to Databricks
iptables -A OUTPUT -p tcp --dport 443 -d <databricks-ip> -j ACCEPT

# Allow inbound to Web UI (restrict to internal network)
iptables -A INPUT -p tcp --dport 8080 -s 10.0.0.0/8 -j ACCEPT

# Allow health checks
iptables -A INPUT -p tcp --dport 8081 -j ACCEPT
```

### Container Security

**Run as non-root:**

```dockerfile
USER connector:connector
```

**Read-only filesystem:**

```yaml
services:
  unified_connector:
    read_only: true
    tmpfs:
      - /tmp
      - /data/spool
```

**Security scanning:**

```bash
# Scan image for vulnerabilities
docker scan unified_connector:latest
```

## Troubleshooting

### Common Issues

#### 1. Colima Not Starting

```bash
# Check status
colima status

# View logs
colima logs

# Restart
colima stop
colima start --cpu 4 --memory 8 --disk 50
```

#### 2. Port Already in Use

```bash
# Check what's using port 8080
lsof -i :8080

# Kill process
kill -9 <PID>

# Or change port in config
```

#### 3. Discovery Not Finding Servers

```bash
# Check network connectivity
docker exec -it unified-connector ping ot-simulator

# Check subnet configuration
docker network inspect opc-ua-zerobus-connector_iot_network

# Update config with correct subnet
```

#### 4. ZeroBus Connection Fails

```bash
# Test Databricks connectivity
curl -v https://adb-xxx.azuredatabricks.net

# Verify credentials
python -m unified_connector.core.credential_manager get zerobus.client_id

# Check logs
docker-compose -f docker-compose.unified.yml logs unified_connector | grep -i zerobus
```

#### 5. High Memory Usage

```bash
# Check metrics
curl http://localhost:8080/api/metrics | jq '.backpressure'

# Reduce queue size in config
backpressure:
  max_queue_size: 10000  # Reduce from 50000

# Enable disk spooling
backpressure:
  disk_spool:
    enabled: true
```

### Debug Mode

Enable verbose logging:

```yaml
connector:
  log_level: "DEBUG"
```

Or via environment:

```bash
docker-compose -f docker-compose.unified.yml up -d \
  -e CONNECTOR_LOG_LEVEL=DEBUG
```

### Getting Help

1. Check logs: `docker-compose -f docker-compose.unified.yml logs -f`
2. Check metrics: `curl http://localhost:8080/api/metrics`
3. Check status: `curl http://localhost:8080/api/status`
4. Review documentation: `unified_connector/README.md`

## Next Steps

- Configure production sources in `config/config.yaml`
- Set up monitoring dashboards (Grafana)
- Configure alerting (Prometheus Alertmanager)
- Implement backup procedures
- Document runbooks for operations team
