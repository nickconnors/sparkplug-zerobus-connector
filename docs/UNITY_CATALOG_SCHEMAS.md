# Unity Catalog table schema

The table the connector writes: one row per Sparkplug B message, through Zerobus.

### Table: `bridge_stage_db.stage_db.sparkplug_raw`

**Purpose:** Bronze layer for Sparkplug B: one row per Sparkplug message (births, deaths and data of edge
nodes and devices), the payload kept whole as Eclipse Tahu's JSON. The connector's `sparkplug` source writes
it (`sparkplug_connector/sparkplug/README.md`); the names are an example, any catalog, schema and table work
(the source's `target_table`).

**Schema:**
```sql
CREATE TABLE IF NOT EXISTS bridge_stage_db.stage_db.sparkplug_raw (
    msg_id STRING COMMENT 'A new UUID per row',
    msg_topic STRING COMMENT 'The MQTT topic: spBv1.0/<group>/<type>/<edge node>[/<device>]',
    namespace STRING COMMENT 'spBv1.0',
    group_id STRING COMMENT 'Sparkplug group id',
    message_type STRING COMMENT 'NBIRTH, NDATA, NDEATH, DBIRTH, DDATA or DDEATH',
    edge_node_id STRING COMMENT 'Sparkplug edge node id',
    device_id STRING COMMENT 'Sparkplug device id (NULL for node messages)',
    msg VARIANT COMMENT 'The payload as Eclipse Tahu JSON',
    inserted_at DECIMAL(38,0) COMMENT 'Receipt time in epoch milliseconds'
)
CLUSTER BY (message_type, group_id, edge_node_id, device_id)
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Bronze layer for Sparkplug B messages - one row per message, JSON records';
```

The connector writes JSON records with confirmed writes (a batch is kept until Zerobus acknowledges it).

**Field Details:**

| Field | Type | Sent as | Description |
|-------|------|---------|-------------|
| `msg_id` | STRING | string | A new UUID per row |
| `msg_topic` | STRING | string | The MQTT topic the message came on |
| `namespace` | STRING | string | Always `spBv1.0` |
| `group_id` | STRING | string | From the topic |
| `message_type` | STRING | string | From the topic: `NBIRTH`, `NDATA`, `NDEATH`, `DBIRTH`, `DDATA`, `DDEATH` |
| `edge_node_id` | STRING | string | From the topic |
| `device_id` | STRING | string or null | From the topic; NULL for node messages |
| `msg` | VARIANT | JSON text | The payload as Tahu JSON: `timestamp`, `seq` and `metrics` (each with `name`, `timestamp`, `dataType`, `value`); GZIP/DEFLATE-compressed payloads are decompressed first |
| `inserted_at` | DECIMAL(38,0) | string of digits | Receipt time in **milliseconds** since epoch (a message held for a missing `seq`: when it is put back in order) |

Host STATE messages, commands (NCMD, DCMD), other namespaces, malformed topics and payloads that can't be
decoded get no row (they're counted in `GET /api/sparkplug/health`).

**Grants** for the service principal that writes it:
```sql
GRANT USE CATALOG ON CATALOG bridge_stage_db TO `<application id>`;
GRANT USE SCHEMA ON SCHEMA bridge_stage_db.stage_db TO `<application id>`;
GRANT MODIFY, SELECT ON TABLE bridge_stage_db.stage_db.sparkplug_raw TO `<application id>`;
```

**Sample Query:** the plain metrics of DDATA messages, one row per metric. A `Template` metric (a UDT
instance) holds its members in `value:metrics`, the same shape one level down.
```sql
SELECT
    timestamp_millis(m.value:timestamp::BIGINT) AS metric_time,
    r.group_id,
    r.edge_node_id,
    r.device_id,
    m.value:name::STRING AS metric,
    m.value:dataType::STRING AS data_type,
    m.value:value AS value
FROM bridge_stage_db.stage_db.sparkplug_raw AS r,
     LATERAL variant_explode(r.msg:metrics) AS m
WHERE r.message_type = 'DDATA'
  AND m.value:dataType::STRING <> 'Template'
ORDER BY metric_time DESC
LIMIT 100;
```

The members of `Template` metrics (UDT instances, how MQTT Transmission publishes Ignition UDTs), one row
per member; a member without a timestamp takes its instance's:
```sql
SELECT
    timestamp_millis(coalesce(t.value:timestamp, m.value:timestamp)::BIGINT) AS metric_time,
    r.group_id,
    r.edge_node_id,
    r.device_id,
    m.value:name::STRING AS instance,
    t.value:name::STRING AS member,
    t.value:dataType::STRING AS data_type,
    t.value:value AS value
FROM bridge_stage_db.stage_db.sparkplug_raw AS r,
     LATERAL variant_explode(r.msg:metrics) AS m,
     LATERAL variant_explode(m.value:value:metrics) AS t
WHERE r.message_type = 'DDATA'
  AND m.value:dataType::STRING = 'Template'
ORDER BY metric_time DESC
LIMIT 100;
```

## Recreating the table

```sql
DROP TABLE IF EXISTS bridge_stage_db.stage_db.sparkplug_raw;
-- then the CREATE TABLE statement above
```

## Version history

| Date | Version | Changes |
|------|---------|---------|
| 2026-10-22 | 3.0 | Sparkplug B only: the other protocols' tables removed with their sources |
| 2026-10-21 | 2.1 | Sparkplug B: the `sparkplug_raw` table |
