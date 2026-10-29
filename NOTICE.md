# Notices

## Eclipse Tahu (EPL-2.0)

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

## Alpine.js and Pico CSS (MIT)

The web page's two vendored files in `sparkplug_connector/web/static/ui/vendor/` are copied unchanged from their
npm packages (versions and checksums: `VERSIONS.md` there):

- `alpine-csp.min.js`: [Alpine.js](https://github.com/alpinejs/alpine)'s CSP build (`@alpinejs/csp` 3.17.4),
  Copyright © 2019-2025 Caleb Porzio and contributors.
- `pico.min.css`: [Pico CSS](https://github.com/picocss/pico) (`@picocss/pico` 2.1.1), Copyright (c) 2019-2024 Pico.

Both under the MIT License:

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of
the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
