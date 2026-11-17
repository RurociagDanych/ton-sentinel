"""Read-only telemetry probes outside PostgreSQL for the /status telemetry block.

Two probes, both opt-in through explicit configuration and both without credentials:

* Kafka end offsets over the internal PLAINTEXT listener: metadata and ListOffsets(latest) only,
  no consumer group, no commits, no record fetches, no SASL/SSL settings. Lag is the log end
  offset minus the next offset after the newest audited record that Spark committed into
  ``stream_batches`` (Spark keeps its own position in the checkpoint and never commits a Kafka
  consumer group, so a consumer-group describe would have nothing to report).
* ``statvfs`` of the database volume mounted read-only into the api container.

Every failure is ``unavailable`` with a fixed reason; exception text never leaves this module.
"""
import os
import re

KAFKA_ENV = "TON_SENTINEL_TELEMETRY_KAFKA_BOOTSTRAP"
VOLUME_ENV = "TON_SENTINEL_TELEMETRY_DB_VOLUME"
KAFKA_TIMEOUT_MS = 3000
_HOST_PORT = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?:([0-9]{1,5})$")
_LAG_SEMANTICS = "log_end_offset_minus_next_offset_after_newest_audited_record"
_LAG_PROVENANCE = ("Kafka ListOffsets(latest) on the internal PLAINTEXT listener, no consumer "
                   "group, no credentials; consumer position from stream_batches audit rows")


def _validate_bootstrap(bootstrap):
    if type(bootstrap) is not str or not bootstrap:
        raise ValueError("kafka bootstrap must be host:port[,host:port]")
    for server in bootstrap.split(","):
        match = _HOST_PORT.match(server)
        if not match or not 1 <= int(match.group(1)) <= 65535:
            raise ValueError("kafka bootstrap must be plain host:port without credentials")
    return bootstrap


def kafka_end_offsets(bootstrap, topics):
    """{topic: {partition: log_end_offset} or None if the topic does not exist}."""
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap.split(","), client_id="ton-sentinel-telemetry",
        group_id=None, enable_auto_commit=False, security_protocol="PLAINTEXT",
        allow_auto_create_topics=False, request_timeout_ms=KAFKA_TIMEOUT_MS,
        bootstrap_timeout_ms=KAFKA_TIMEOUT_MS, metadata_max_age_ms=KAFKA_TIMEOUT_MS)
    try:
        result = {}
        for topic in topics:
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                result[topic] = None
                continue
            ends = consumer.end_offsets([TopicPartition(topic, p) for p in sorted(partitions)])
            result[topic] = {tp.partition: int(offset) for tp, offset in ends.items()}
        return result
    finally:
        consumer.close(autocommit=False)


def _unavailable(reason, provenance):
    return {"status": "unavailable", "value": None, "reason": reason, "provenance": provenance}


class TelemetryProbes:
    """Applies the configured probes to a telemetry dict from ``serving.read_telemetry``."""

    def __init__(self, *, kafka_bootstrap=None, database_volume=None, end_offsets=None,
                 statvfs=None, ismount=None):
        self.kafka_bootstrap = (None if kafka_bootstrap is None
                                else _validate_bootstrap(kafka_bootstrap))
        if database_volume is not None and (type(database_volume) is not str
                                            or not os.path.isabs(database_volume)):
            raise ValueError("database volume must be an absolute path")
        self.database_volume = database_volume
        self._end_offsets = end_offsets or (
            lambda topics: kafka_end_offsets(self.kafka_bootstrap, topics))
        self._statvfs = statvfs or os.statvfs
        self._ismount = ismount or os.path.ismount

    def apply(self, telemetry):
        if not isinstance(telemetry, dict):
            return telemetry
        result = dict(telemetry)
        if self.kafka_bootstrap is not None:
            result["kafka_lag"] = self._kafka_lag(telemetry.get("spark_progress"))
        if self.database_volume is not None:
            result["disk"] = self._disk()
        return result

    def _disk(self):
        provenance = "statvfs on the read-only database volume mount"
        try:
            if not self._ismount(self.database_volume):
                return _unavailable("path_is_not_a_mount", provenance)
            stat = self._statvfs(self.database_volume)
            size = int(stat.f_frsize)
            total, free, available = (size * int(stat.f_blocks), size * int(stat.f_bfree),
                                      size * int(stat.f_bavail))
        except Exception:
            return _unavailable("statvfs_failed", provenance)
        if size <= 0 or total <= 0:
            return _unavailable("statvfs_failed", provenance)
        return {"status": "observed", "total_bytes": total, "free_bytes": free,
                "available_bytes": available, "scope": "filesystem_backing_database_volume",
                "provenance": provenance}

    def _kafka_lag(self, progress):
        base = {"semantics": _LAG_SEMANTICS, "provenance": _LAG_PROVENANCE}
        state = (progress or {}).get("status")
        if state == "none_recorded":
            return {"status": "none_recorded", "value": None, "reason": "no_spark_batch_recorded",
                    "provenance": _LAG_PROVENANCE}
        if state != "observed" or not isinstance(progress.get("value"), list):
            return _unavailable("consumer_position_unavailable", _LAG_PROVENANCE)
        queries = progress["value"]
        topics = sorted({q["topic"] for q in queries if q.get("offsets_audited")})
        try:
            ends = self._end_offsets(topics) if topics else {}
        except Exception:
            return _unavailable("broker_unreachable", _LAG_PROVENANCE)
        value = [self._query_lag(query, ends) for query in queries]
        if not any(entry["status"] == "observed" for entry in value):
            return dict(base, status="unavailable", reason="no_measurable_query", value=None,
                        queries=value)
        return dict(base, status="observed", value=value)

    @staticmethod
    def _query_lag(query, ends):
        entry = {"query_id": query["query_id"], "topic": query["topic"]}

        def unavailable(reason):
            return dict(entry, status="unavailable", reason=reason, total_lag_records=None,
                        partitions=None)

        if not query.get("offsets_audited"):
            return unavailable("latest_batch_not_audited")
        log = ends.get(query["topic"])
        if not log:
            return unavailable("topic_not_found")
        positions = {p["partition"]: p["next_offset"] for p in query.get("partitions") or []}
        if set(positions) - set(log):
            return unavailable("recorded_partition_not_in_topic")
        partitions = []
        for partition in sorted(log):
            end, position = int(log[partition]), positions.get(partition)
            if position is not None and position > end:
                return unavailable("position_ahead_of_log_end")
            partitions.append({"partition": partition, "log_end_offset": end,
                               "next_offset": position,
                               "lag_records": None if position is None else end - position})
        result = dict(entry, status="observed", partitions=partitions)
        if any(p["lag_records"] is None for p in partitions):
            result.update(total_lag_records=None,
                          total_reason="position_not_recorded_for_some_partitions")
        else:
            result["total_lag_records"] = sum(p["lag_records"] for p in partitions)
        return result


def probes_from_environ(environ):
    """Probes configured by explicit environment variables only; ``None`` when none is set."""
    bootstrap = environ.get(KAFKA_ENV) or None
    volume = environ.get(VOLUME_ENV) or None
    if bootstrap is None and volume is None:
        return None
    return TelemetryProbes(kafka_bootstrap=bootstrap, database_volume=volume)
