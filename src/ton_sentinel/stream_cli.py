"""Bounded Spark Structured Streaming consumer for canonical outbox records."""
from __future__ import annotations

import argparse
import json
import os
import uuid
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ConsumerConfig:
    dsn: str
    bootstrap_servers: str
    topic: str
    checkpoint: str
    topic_incarnation: str
    replay_epoch: int
    environment_id: str
    query_id: str
    pipeline_epoch: str
    max_offsets_per_trigger: int = 500
    # Extra Kafka client TLS settings (key, value) passed through as ``kafka.<key>``; the values
    # are file paths/types only, never secrets. Default () keeps the previous plaintext behaviour.
    kafka_options: tuple[tuple[str, str], ...] = ()
    # "earliest" for a normal lineage; a resumed lineage starts exactly at the
    # recovery cut's end offsets E (``recovery.resumed_consumer_config``). Never "latest".
    starting_offsets: str = "earliest"
    # Windows scored by the post-promotion
    # hook after each committed microbatch. ``None`` turns the hook off; otherwise 1..10000.
    score_limit_per_cycle: int | None = 50

    def validate(self) -> Path:
        if type(self.dsn) is not str or not self.dsn.strip():
            raise ValueError("an explicit PostgreSQL DSN is required")
        if type(self.bootstrap_servers) is not str or not self.bootstrap_servers.strip():
            raise ValueError("explicit Kafka bootstrap servers are required")
        if (type(self.topic) is not str or not self.topic or len(self.topic) > 249
                or any(char.isspace() for char in self.topic)):
            raise ValueError("a valid explicit Kafka topic is required")
        if (type(self.topic_incarnation) is not str or not self.topic_incarnation
                or len(self.topic_incarnation) > 256):
            raise ValueError("an explicit topic incarnation is required")
        if type(self.replay_epoch) is not int or self.replay_epoch < 0:
            raise ValueError("replay epoch must be a nonnegative integer")
        for name in ("environment_id", "query_id", "pipeline_epoch"):
            value = getattr(self, name)
            if type(value) is not str or not value.strip() or len(value) > 256:
                raise ValueError(f"an explicit {name} of at most 256 characters is required")
        if (type(self.max_offsets_per_trigger) is not int
                or not 1 <= self.max_offsets_per_trigger <= 500):
            raise ValueError("max_offsets_per_trigger must be 1..500")
        for item in self.kafka_options:
            if (type(item) is not tuple or len(item) != 2 or type(item[0]) is not str
                    or type(item[1]) is not str or not item[1]
                    or not (item[0] == "security.protocol" or item[0].startswith("ssl."))):
                raise ValueError("kafka option must be security.protocol or ssl.* with a text value")
        if self.score_limit_per_cycle is not None and (
                type(self.score_limit_per_cycle) is not int
                or not 1 <= self.score_limit_per_cycle <= 10000):
            raise ValueError("score_limit_per_cycle must be None or 1..10000")
        starting_offsets_by_partition(self)
        if type(self.checkpoint) is not str or not self.checkpoint.strip():
            raise ValueError("an explicit durable Spark checkpoint path is required")
        path = Path(self.checkpoint).expanduser()
        if not path.is_absolute() or path == Path("/"):
            raise ValueError("checkpoint path must be an absolute persistent directory")
        if path.exists() and not path.is_dir():
            raise ValueError("checkpoint path must be a directory")
        return path


def starting_offsets_by_partition(config: ConsumerConfig) -> dict[int, int] | None:
    """None for "earliest"; else the exact per-partition start E (partitions 0..n-1, E >= 0)."""
    value = config.starting_offsets
    if value == "earliest":
        return None
    error = ValueError("starting_offsets must be \"earliest\" or the exact recovery cut end "
                       "offsets {topic: {partition: offset >= 0}} for partitions 0..n-1")
    if type(value) is not str:
        raise error
    try:
        parsed = json.loads(value)
    except ValueError:
        raise error from None
    if type(parsed) is not dict or set(parsed) != {config.topic}:
        raise error
    offsets = parsed[config.topic]
    if type(offsets) is not dict or not offsets:
        raise error
    result = {}
    for key, offset in offsets.items():
        if type(key) is not str or not key.isdigit() or type(offset) is not int or offset < 0:
            raise error
        result[int(key)] = offset
    if sorted(result) != list(range(len(result))):
        raise error
    return result


def kafka_reader_options(config: ConsumerConfig) -> dict[str, str]:
    options = {
        "kafka.bootstrap.servers": config.bootstrap_servers,
        "subscribe": config.topic,
        "startingOffsets": config.starting_offsets,
        "failOnDataLoss": "true",
        "maxOffsetsPerTrigger": str(config.max_offsets_per_trigger),
    }
    for key, value in config.kafka_options:
        options["kafka." + key] = value
    return options


def score_after_promotion(connection, promoted: int, *, limit: int | None, owner: str,
                          acquire=None, score=None) -> dict | None:
    """Score the backlog right after a committed promotion; never raises.

    Runs strictly after the feature promotion transaction has committed, on a fresh bounded
    lease and in its own transactions, so a scoring failure (including a lost lease) cannot
    undo served features: it abandons only the scoring transaction and is reported by class
    name (messages can carry DSNs). Returns ``None`` when disabled (``limit is None``) or
    when nothing was promoted; at most ``limit`` windows are scored (``score_pending``).
    """
    if limit is None or not promoted:
        return None
    try:
        if acquire is None:
            from ton_sentinel.db.fencing import acquire_transition as acquire
        if score is None:
            from ton_sentinel.db.work import score_pending as score
        lease = acquire(connection, owner, 300)
        report = score(connection, lease, network=None, limit=limit)
        connection.commit()
        return {"status": "scored", **report}
    except Exception as exc:  # noqa: BLE001 - promotion is already durable; report class only
        try:
            connection.rollback()
        except Exception:  # noqa: BLE001
            pass
        return {"status": "failed", "error": type(exc).__name__}


def promote_then_score(connection, promote, *, limit: int | None, owner: str,
                       acquire=None, score=None) -> tuple[int, dict | None]:
    """Run ``promote()`` (a committed feature promotion); only if it returns normally with
    promoted windows, run the bounded scoring hook. A promotion that raises schedules nothing."""
    promoted = promote()
    return promoted, score_after_promotion(connection, promoted, limit=limit, owner=owner,
                                           acquire=acquire, score=score)


def run_consumer(config: ConsumerConfig) -> None:
    """Run one bounded availableNow Kafka pass using the durable checkpoint."""
    checkpoint = config.validate()
    import psycopg

    from ton_sentinel.db.stream_batches import bind_checkpoint, checkpoint_identity

    if config.starting_offsets != "earliest":
        # Explicit coordinates are only ever a durable recovery resumption's E; never guessed.
        # Checked before the checkpoint is bound, so a refused start leaves no lineage behind.
        from ton_sentinel.recovery import check_resumed_start
        with psycopg.connect(config.dsn) as connection:
            check_resumed_start(connection, config)

    # Fail closed before Spark can read or mutate an incompatible checkpoint.
    bind_checkpoint(checkpoint, checkpoint_identity(
        environment_id=config.environment_id, query_id=config.query_id,
        pipeline_epoch=config.pipeline_epoch, topic=config.topic,
        topic_incarnation=config.topic_incarnation, replay_epoch=config.replay_epoch,
    ))

    from pyspark.sql import SparkSession

    from ton_sentinel.db.fencing import acquire_transition
    from ton_sentinel.spark.streaming import process_canonical_microbatch

    spark = (SparkSession.builder.appName("ton-sentinel-canonical-stream")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "1")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        source = spark.readStream.format("kafka").options(**kafka_reader_options(config)).load()
        owner = "canonical-stream-" + uuid.uuid4().hex

        def apply_batch(batch_df, batch_id: int) -> None:
            # A fresh, bounded lease is acquired per actual microbatch. The
            # adapter delegates to the fenced freeze/compute/promote path.
            with psycopg.connect(config.dsn) as connection:
                lease = acquire_transition(connection, owner, ttl_seconds=300)
                # After the promotion committed, score within a small budget.
                promote_then_score(
                    connection,
                    lambda: process_canonical_microbatch(
                        connection, lease, spark, batch_df,
                        topic_incarnation=config.topic_incarnation,
                        replay_epoch=config.replay_epoch,
                        batch_id=batch_id,
                        environment_id=config.environment_id,
                        query_id=config.query_id,
                        pipeline_epoch=config.pipeline_epoch,
                        topic=config.topic,
                        max_rows=config.max_offsets_per_trigger,
                    ),
                    limit=config.score_limit_per_cycle, owner=owner)

        query = (source.writeStream.foreachBatch(apply_batch)
                 .option("checkpointLocation", str(checkpoint))
                 .trigger(availableNow=True).start())
        try:
            query.awaitTermination()
        finally:
            if query.isActive:
                query.stop()
    finally:
        spark.stop()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consume bounded canonical outbox Kafka batches into serving rows."
    )
    parser.add_argument("--dsn", default=os.environ.get("TON_SENTINEL_DSN"),
                        help="PostgreSQL DSN (or TON_SENTINEL_DSN)")
    parser.add_argument("--bootstrap-servers", default=os.environ.get("TON_SENTINEL_KAFKA_BOOTSTRAP"),
                        help="Kafka bootstrap servers (or TON_SENTINEL_KAFKA_BOOTSTRAP)")
    parser.add_argument("--topic", default=os.environ.get("TON_SENTINEL_KAFKA_TOPIC"),
                        help="canonical topic (or TON_SENTINEL_KAFKA_TOPIC)")
    parser.add_argument("--checkpoint", default=os.environ.get("TON_SENTINEL_CHECKPOINT"),
                        help="absolute persistent checkpoint directory (or TON_SENTINEL_CHECKPOINT)")
    parser.add_argument("--topic-incarnation", default=os.environ.get("TON_SENTINEL_TOPIC_INCARNATION"),
                        help="explicit configured topic incarnation")
    parser.add_argument("--replay-epoch", type=int, default=os.environ.get("TON_SENTINEL_REPLAY_EPOCH"),
                        help="explicit configured replay epoch")
    parser.add_argument("--environment-id", default=os.environ.get("TON_SENTINEL_ENVIRONMENT_ID"),
                        help="non-secret deployment environment identity")
    parser.add_argument("--query-id", default=os.environ.get("TON_SENTINEL_QUERY_ID"),
                        help="stable non-secret streaming query identity")
    parser.add_argument("--pipeline-epoch", default=os.environ.get("TON_SENTINEL_PIPELINE_EPOCH"),
                        help="explicit pipeline epoch; change it for a deliberate replay")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=500,
                        help="bounded Kafka offsets per microbatch (1..500)")
    parser.add_argument("--starting-offsets", default="earliest",
                        help="earliest, or the exact recovery cut end offsets JSON of a "
                             "durable resumption")
    parser.add_argument("--kafka-option", action="append", default=[], type=_kafka_option,
                        metavar="KEY=VALUE",
                        help="Kafka client TLS option (security.protocol or ssl.*), repeatable")
    parser.add_argument("--score-limit-per-cycle", type=int, default=50,
                        help="windows scored after each promoted microbatch (1..10000, default 50)")
    parser.add_argument("--no-score-after-promotion", action="store_true",
                        help="turn the post-promotion scoring hook off")
    return parser


def _kafka_option(text: str) -> tuple[str, str]:
    key, sep, value = text.partition("=")
    if not sep or not key or not value:
        raise argparse.ArgumentTypeError("expected KEY=VALUE")
    return key, value


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.replay_epoch is None:
        raise SystemExit("--replay-epoch (or TON_SENTINEL_REPLAY_EPOCH) is required")
    config = ConsumerConfig(
        dsn=args.dsn or "", bootstrap_servers=args.bootstrap_servers or "",
        topic=args.topic or "", checkpoint=args.checkpoint or "",
        topic_incarnation=args.topic_incarnation or "", replay_epoch=args.replay_epoch,
        environment_id=args.environment_id or "", query_id=args.query_id or "",
        pipeline_epoch=args.pipeline_epoch or "",
        max_offsets_per_trigger=args.max_offsets_per_trigger,
        kafka_options=tuple(args.kafka_option),
        starting_offsets=args.starting_offsets,
        score_limit_per_cycle=None if args.no_score_after_promotion else args.score_limit_per_cycle,
    )
    config.validate()
    run_consumer(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
