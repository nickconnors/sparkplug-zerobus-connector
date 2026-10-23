"""Java Eclipse Tahu 1.0.21 through JPype: the reference the Python translation is tested against.

Runs Tahu's own decode and JSON path (``SparkplugBPayloadDecoder.buildFromByteArray``, then
``PayloadUtil.decompress`` for a compressed payload, then ``PayloadUtil.toJsonString``).

- Java runtime: ``TAHU_JVM`` (path of ``libjvm``/``libjli``), else JPype's default. The comparison is
  meant for **Java 17**, whose output the connector reproduces (number spelling and HashMap order differ on 19+).
- Jars: ``TAHU_JAVA_CLASSPATH`` (``os.pathsep``-separated), else the Gradle cache (``~/.gradle``),
  where any Gradle build that depends on ``org.eclipse.tahu:tahu-core:1.0.21`` puts them.
- Tahu's host library for ``TahuHost`` (a host application's decisions): ``tahu-host-1.0.21.jar`` from the Gradle cache
  or the local Maven repository (``~/.m2/repository/org/eclipse/tahu/tahu-host/1.0.21/``; any Maven mirror has it);
  without it only ``TahuHost`` is unavailable.
- Never hand it a truncated DEFLATE body: Java Tahu loops forever on one (``to_tahu_json`` refuses).
"""
from __future__ import annotations

import glob
import os
import zlib

from sparkplug_connector.sparkplug import sparkplug_b_decode_pb2 as dpb

GRADLE_CACHE = os.path.expanduser("~/.gradle/caches/modules-2/files-2.1")
JARS = (
    "org.eclipse.tahu/tahu-core/1.0.21/*/tahu-core-1.0.21.jar",
    "com.google.protobuf/protobuf-java/4.33.0/*/protobuf-java-4.33.0.jar",
    "com.fasterxml.jackson.core/jackson-core/2.22.3/*/jackson-core-2.22.3.jar",
    "com.fasterxml.jackson.core/jackson-databind/2.22.3/*/jackson-databind-2.22.3.jar",
    "com.fasterxml.jackson.core/jackson-annotations/2.22/*/jackson-annotations-2.22.jar",
    "org.slf4j/slf4j-api/2.0.17/*/slf4j-api-2.0.17.jar",
    "org.eclipse.paho/org.eclipse.paho.client.mqttv3/1.2.5/*/org.eclipse.paho.client.mqttv3-1.2.5.jar",
)

TAHU_HOST_JARS = (os.path.join(GRADLE_CACHE, "org.eclipse.tahu/tahu-host/1.0.21/*/tahu-host-1.0.21.jar"),
                  os.path.expanduser("~/.m2/repository/org/eclipse/tahu/tahu-host/1.0.21/tahu-host-1.0.21.jar"))

_java: dict = {}


class JavaTahuError(Exception):
    """Java Tahu threw (the payload can't be decoded or serialized)."""


def _classpath() -> list[str]:
    configured = os.environ.get("TAHU_JAVA_CLASSPATH")
    if configured:
        return configured.split(os.pathsep)
    jars = []
    for pattern in JARS:
        found = glob.glob(os.path.join(GRADLE_CACHE, pattern))
        if not found:
            raise FileNotFoundError(f"{pattern} not in {GRADLE_CACHE} (resolve tahu-core 1.0.21 with Gradle once, or set TAHU_JAVA_CLASSPATH)")
        jars.append(found[0])
    host = [j for pattern in TAHU_HOST_JARS for j in glob.glob(pattern)]
    return jars + host[:1]


def _start() -> None:
    if _java:
        return
    import jpype

    if not jpype.isJVMStarted():
        jvm = os.environ.get("TAHU_JVM") or jpype.getDefaultJVMPath()
        jpype.startJVM(jvm, "-Xmx512m", "-Dslf4j.internal.verbosity=ERROR", classpath=_classpath())
    _java["decoder"] = jpype.JClass("org.eclipse.tahu.message.SparkplugBPayloadDecoder")()
    _java["util"] = jpype.JClass("org.eclipse.tahu.util.PayloadUtil")
    _java["bytes"] = jpype.JArray(jpype.JByte)
    _java["version"] = str(jpype.JClass("java.lang.System").getProperty("java.version"))
    _java["home"] = str(jpype.JClass("java.lang.System").getProperty("java.home"))


def unavailable_reason() -> str | None:
    """Why the reference can't run here (None if it can)."""
    try:
        _start()
    except Exception as e:  # noqa: BLE001 - any reason to skip
        return f"Java Tahu reference not available: {type(e).__name__}: {e}"
    return None


def java_version() -> str:
    _start()
    return _java["version"]


def java_home() -> str:
    _start()
    return _java["home"]


def to_tahu_json(payload: bytes, types=None) -> str:
    """Java Tahu's JSON for the payload; ``types``: a ``MetricTypes`` (the births' types), as a ``MetricDataTypeMap``."""
    _start()
    if _would_hang(payload):
        raise ValueError("truncated DEFLATE body: Java Tahu would loop forever")
    import jpype

    try:
        java_types = None if types is None else metric_data_type_map(types)
        decoded = _java["decoder"].buildFromByteArray(_java["bytes"](payload), java_types)
        if str(decoded.getUuid()) == str(_java["util"].UUID_COMPRESSED):
            decoded = _java["util"].decompress(decoded, java_types)
        return str(_java["util"].toJsonString(decoded))
    except jpype.JException as e:
        raise JavaTahuError(f"{e.getClass().getName()}: {e.getMessage()}") from None


def metric_data_type_map(types):
    """A Java ``MetricDataTypeMap`` with the same entries as a ``MetricTypes``."""
    _start()
    import jpype

    m = jpype.JClass("org.eclipse.tahu.model.MetricDataTypeMap")()
    data_type = jpype.JClass("org.eclipse.tahu.message.model.MetricDataType")
    for name, code in types.names.items():
        m.addMetricDataType(jpype.JString(name), data_type.fromInteger(code))
    for alias, code in types.aliases.items():
        m.addMetricDataType(jpype.JLong(alias), data_type.fromInteger(code))
    return m


def _would_hang(payload: bytes) -> bool:
    """A compressed payload whose DEFLATE body never finishes (Tahu's inflate loop has no exit then)."""
    try:
        p = dpb.Payload.FromString(payload)
    except Exception:  # noqa: BLE001 - Java will reject it too
        return False
    if p.uuid != b"SPBV1.0_COMPRESSED" or not p.HasField("body"):
        return False
    algorithm = b"DEFLATE"
    for m in p.metrics:
        if not m.HasField("name") or not (1 <= m.datatype <= 34 and m.datatype not in (20, 21)):
            return False                             # Java throws (NullPointerException) before inflating
        if m.name == b"algorithm":
            if m.is_null or m.datatype not in (12, 14, 15) or m.string_value not in (b"GZIP", b"DEFLATE"):
                return False                         # Java throws (no such algorithm) before inflating
            algorithm = m.string_value
    if algorithm != b"DEFLATE":
        return False
    d = zlib.decompressobj()
    try:
        d.decompress(p.body)
    except zlib.error as e:
        return "Error 2 " in str(e)                  # a preset dictionary: Java's loop never ends either
    return not d.eof


def host_unavailable_reason() -> str | None:
    """Why ``TahuHost`` can't run here (None if it can)."""
    reason = unavailable_reason()
    if reason:
        return reason
    import jpype

    try:
        jpype.JClass("org.eclipse.tahu.host.seq.SequenceReorderManager")
    except Exception:  # noqa: BLE001 - the jar isn't on the class path
        return f"Tahu's host library not found ({' or '.join(TAHU_HOST_JARS)})"
    return None


class TahuHost:
    """Tahu 1.0.21's host decisions: messages through a ``SequenceReorderManager`` (its timer only with
    ``timeout_ms``) into ``TahuPayloadHandler``, with Tahu's own singletons (so each test uses its own group).
    ``rows``: (topic, Tahu's JSON of the payload) per message handed to the event handler (``onMessage``), in order;
    ``requests``: the NCMD topics of the rebirth requests."""

    def __init__(self, timeout_ms: int | None = None):
        _start()
        import jpype

        self.rows: list[tuple[str, str]] = []
        self.requests: list[str] = []
        rows, requests, util = self.rows, self.requests, _java["util"]

        @jpype.JImplements("org.eclipse.tahu.host.api.MultiHostApplicationEventHandler")
        class Handler:
            @jpype.JOverride
            def onMessage(self, server, descriptor, message):
                rows.append((str(message.getTopic().toString()), str(util.toJsonString(message.getPayload()))))

            def _ignore(self, *args):
                pass

            onConnect = onDisconnect = onNodeBirthArrived = onNodeBirthComplete = onNodeDataArrived = \
                onNodeDataComplete = onNodeDeath = onNodeDeathComplete = onDeviceBirthArrived = onDeviceBirthComplete = \
                onDeviceDataArrived = onDeviceDataComplete = onDeviceDeath = onDeviceDeathComplete = onBirthMetric = \
                onDataMetric = onStale = jpype.JOverride(_ignore)

        @jpype.JImplements("org.eclipse.tahu.host.CommandPublisher")
        class Publisher:
            @jpype.JOverride
            def publishCommand(self, *args):
                requests.append(str(args[-2].toString()))

        self._handler, self._publisher = Handler(), Publisher()
        self._manager = jpype.JClass("org.eclipse.tahu.host.seq.SequenceReorderManager")()
        self._manager.init(self._handler, self._publisher, _java["decoder"], jpype.JLong(timeout_ms or 5000))
        if timeout_ms:
            self._manager.start()
        concurrent = "java.util.concurrent."
        self._executor = jpype.JClass(concurrent + "ThreadPoolExecutor")(
            1, 1, jpype.JLong(0), jpype.JClass(concurrent + "TimeUnit").MILLISECONDS, jpype.JClass(concurrent + "LinkedBlockingQueue")())
        self._server = jpype.JClass("org.eclipse.tahu.mqtt.MqttServerName")("test")
        self._client = jpype.JClass("org.eclipse.tahu.mqtt.MqttClientId")("test-host", False)
        self._split = jpype.JClass("org.eclipse.tahu.util.TopicUtil").getSplitTopic
        self._mqtt = jpype.JClass("org.eclipse.paho.client.mqttv3.MqttMessage")
        self._nano = jpype.JClass("java.lang.System").nanoTime

    def send(self, topic: str, payload: bytes) -> None:
        """One message as it arrives (TahuHostCallback.messageArrived with sequence reordering on)."""
        import jpype

        try:
            self._manager.handlePayload(None, self._executor, topic, self._split(topic), self._mqtt(_java["bytes"](payload)),
                                        self._server, self._client, self._nano())
        except jpype.JException:
            pass                                    # TahuHostCallback catches and logs it: the message is dropped

    def drain(self) -> None:
        """Until the payload handler has handled everything handed to it."""
        import jpype

        @jpype.JImplements("java.lang.Runnable")
        class Noop:
            @jpype.JOverride
            def run(self):
                pass

        self._executor.submit(Noop()).get()

    def close(self) -> None:
        self._manager.stop()
        self._executor.shutdownNow()
