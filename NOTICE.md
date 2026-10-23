# Notice: Eclipse Tahu

The Sparkplug B decoder and sequencer in `sparkplug_connector/sparkplug/` are derived from
[Eclipse Tahu](https://github.com/eclipse-tahu/tahu) 1.0.21 (tag `v1.0.21`), Copyright (c) 2014-2022 Cirrus
Link Solutions and others, made available under the [Eclipse Public License 2.0](https://www.eclipse.org/legal/epl-2.0/)
(`SPDX-License-Identifier: EPL-2.0`). These files stay under EPL-2.0:

| File (in `sparkplug_connector/sparkplug/`) | Derived from |
|---|---|
| `sparkplug_b.proto` | `sparkplug_b/sparkplug_b.proto` (copied unchanged) |
| `sparkplug_b_pb2.py` | generated from `sparkplug_b.proto` with `grpcio-tools` 1.78.0 |
| `sparkplug_b_decode.proto`, `sparkplug_b_decode_pb2.py` | a decode-only copy of `sparkplug_b.proto` with every `string` field as `bytes`, generated the same way |
| `tahu_json.py` | a Python translation of `org.eclipse.tahu.message.SparkplugBPayloadDecoder`, the JSON shape of the `org.eclipse.tahu.message.model` classes, `org.eclipse.tahu.json.FileSerializer` and `org.eclipse.tahu.util.PayloadUtil.decompress` |
| `sequencer.py` | a Python translation of the decisions in Tahu's host library (`tahu-host`): `org.eclipse.tahu.host.seq.SequenceReorderManager` and `SequenceReorderMap`, `org.eclipse.tahu.host.TahuPayloadHandler`, `org.eclipse.tahu.host.manager.SparkplugEdgeNode` and `SparkplugDevice`, `org.eclipse.tahu.host.model.HostApplicationMetricMap` |

Details, and the files that are not derived from Tahu: [sparkplug_connector/sparkplug/NOTICE.md](sparkplug_connector/sparkplug/NOTICE.md).
Keep this notice with these files when you redistribute them.
