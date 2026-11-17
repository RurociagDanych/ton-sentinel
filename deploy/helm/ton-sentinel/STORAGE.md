# Offline storage contract — not deployment approval

Status: inert correction candidate; G2/G5 remain held. Local-PV capacities and
namespace request quotas are binding/scheduling metadata, NOT filesystem quotas.
No filesystem has been prepared, mounted, resized, or qualified by this change.

## Physical allocation

`storage-budget.json` is the chart's reviewed-candidate budget. The dedicated data volume is
conservatively treated as 150,000,000,000 bytes, not 150 GiB (a chart contract, not a cloud quota; measured local host filesystem: 501.8 GB, 276.0 GB free, `local_host_measurement` in `deploy/workload/acceptance.json`). Six independent ext4
partitions on the dedicated data disk are required:

| Writer | Partition size (GiB) | Mount under /srv/ton-sentinel |
| --- | ---: | --- |
| PostgreSQL (including WAL/temp) | 40 | postgres |
| Kafka (data, internal topics, metadata) | 20 | kafka |
| Spark checkpoints | 12 | checkpoints |
| Scratch | 8 | scratch |
| Metrics | 1 | metrics |
| K3s data/images | 16 | k3s |

Total: 97 GiB; at least 16 GiB must remain outside those six partitions. Filesystem
metadata/reserved blocks reduce each writer's usable space; declared PV capacity
is an upper ceiling, not a guarantee of available payload capacity. A full writer
partition cannot grow into another partition or the unpartitioned reserve.
This does not prevent ENOSPC, prove application recovery, or constrain a malicious
host administrator. Host logs/boot usage require separate qualification.

No host bootstrap is part of this repository. The former cloud bootstrap candidate
(mount units and read-only storage verifier) was removed with the dropped cloud
target (owner decision 2026-11-13). Any chosen target host needs its own reviewed
partition, mount and verification procedure before activation. Nothing here
creates directories, changes ownership, formats, mounts, repairs, resizes, or
changes quotas. Image-specific tool/device behavior and host systemd
dependencies remain unqualified.

## Kafka aggregate contract

Exactly four single-partition topics are budgeted: ton.pool_state.v1 and
ton.swap_events.v1 at 2 GiB each; __consumer_offsets and __transaction_state at
4 GiB each. Add two 128 MiB overshoot segments for EACH partition and 2 GiB for
KRaft metadata/index/checkpoint/headroom: 12 + 1 + 2 = 15 GiB, below the 20 GiB
Kafka partition. Overshoot allowances are planning allowances, NOT hard limits
on broker retention lag or compacted internal topics. The separate 20 GiB
filesystem is the aggregate physical upper bound.

The inert broker template sets segment sizes, internal-topic partition defaults,
replication factor one, disables data-topic auto-creation, and lowers ordinary
per-partition retention to 2 GiB. Broker defaults do NOT prohibit administrative
creation of additional topics/partitions or bound compacted internal topic growth.
No runtime topic provisioning, ACL enforcement, ongoing topology audit, or image
configuration compatibility is claimed. Those are explicit activation blockers.

## Fail-closed admission and activation boundary

Chart schema AND template admission reject nonzero replicas (including rendering
with schema validation absent). Admission also rejects missing/invalid contracts,
layout drift, duplicate/extra topics, changed cardinality, Kafka aggregate budget
overflow, or a shared-disk budget without reserve. Defaults render no objects.
Synthetic fixtures can render only zero-replica resources; they are not evidence
of gate authorization. App and maintenance entrypoints remain held.

No host-side guard or systemd unit is supplied by this chart. No enable/start
command is supplied, and manually starting host services is NOT authorized by
these files.

Before any activation, obtain independent review and separate exact authorization
for partition preparation, filesystem ownership, verified image tooling, Kafka
creation/alter authorization and topology checks, retention/compaction behavior,
ENOSPC recovery and real restart/PVC tests. Removing the hold is a separate change,
not an option or fabricated evidence token. This document does not close the open
storage hold; independent review owns disposition.
