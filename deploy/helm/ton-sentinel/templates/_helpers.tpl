{{- define "sentinel.name" -}}{{- printf "%s-ton-sentinel" .Release.Name | trunc 45 | trimSuffix "-" -}}{{- end -}}
{{- define "sentinel.namespace" -}}{{ include "sentinel.name" . }}{{- end -}}
{{- define "sentinel.localK3s" -}}{{- if and .Values.local .Values.local.enabled -}}true{{- end -}}{{- end -}}
{{- define "sentinel.arch" -}}{{- if include "sentinel.localK3s" . -}}{{ .Values.local.nodeArch }}{{- else -}}arm64{{- end -}}{{- end -}}
{{- define "sentinel.admission" -}}
{{- if .Values.enabled -}}
{{- if and (include "sentinel.localK3s" .) (not (has (toString .Values.replicas) (list "0" "1"))) -}}
{{- fail "BLOCKED: local K3s mode admits replicas 0 or 1 only" -}}
{{- end -}}
{{- if and (not (include "sentinel.localK3s" .)) (ne (toString .Values.replicas) "0") -}}
{{- fail "BLOCKED: offline storage enforcement hold; replicas must remain zero pending independently reviewed filesystem preparation and Kafka topology qualification" -}}
{{- end -}}
{{- include "sentinel.storageAdmission" . -}}
{{- if lt (int .Values.admission.allocatableMiB) (add 6144 (int .Values.admission.osReserveMiB)) -}}
{{- fail "BLOCKED: provisional 6144Mi pod ceiling plus OS/K3s reserve exceeds supplied allocatable memory; G2 must measure actual footprint" -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- define "sentinel.storageAdmission" -}}
{{- $raw := .Files.Get "storage-budget.json" -}}
{{- if not $raw -}}{{- fail "BLOCKED: missing offline storage contract" -}}{{- end -}}
{{- /* JSON decoding uses float64. Reject fractional/exponent spellings before
      decoding can round them, then check types/range before any int64 coercion.
      Strip JSON strings first so punctuation in names does not affect numbers. */ -}}
{{- $tokens := regexReplaceAll `"([^"\\]|\\.)*"` $raw "" -}}
{{- if regexMatch `[^0-9[:space:]{},:\[\]]` $tokens -}}
{{- fail "BLOCKED: storage numeric contract requires positive integer JSON literals" -}}
{{- end -}}
{{- $budget := mustFromJson $raw -}}
{{- $numbers := list $budget.dataVolumeBytes $budget.reserveBytes $budget.kafka.segmentBytes $budget.kafka.overshootSegmentsPerPartition $budget.kafka.metadataReserveBytes -}}
{{- range $size := $budget.writersGiB -}}
{{- $numbers = append $numbers $size -}}
{{- end -}}
{{- range $topic := $budget.kafka.topics -}}
{{- $numbers = append $numbers $topic.partitions -}}
{{- $numbers = append $numbers $topic.budgetPerPartitionBytes -}}
{{- end -}}
{{- range $value := $numbers -}}
{{- if not (kindIs "float64" $value) -}}
{{- fail "BLOCKED: storage numeric contract requires numeric JSON types, not strings or missing values" -}}
{{- end -}}
{{- if or (le $value (float64 0)) (gt $value (float64 150000000000)) -}}
{{- fail "BLOCKED: storage numeric contract must be within the positive 150GB physical bound before conversion" -}}
{{- end -}}
{{- end -}}
{{- $gib := int64 1073741824 -}}
{{- $allocated := int64 0 -}}
{{- range $size := $budget.writersGiB -}}
{{- if le (int64 $size) 0 -}}{{- fail "BLOCKED: invalid shared disk budget" -}}{{- end -}}
{{- $allocated = add $allocated (mul (int64 $size) $gib) -}}
{{- end -}}
{{- if or (ne (int64 $budget.dataVolumeBytes) (int64 150000000000)) (lt (int64 $budget.reserveBytes) (mul 16 $gib)) (gt (add $allocated (int64 $budget.reserveBytes)) (int64 $budget.dataVolumeBytes)) -}}
{{- fail "BLOCKED: shared disk budget exceeds conservative 150GB volume or omits reserve" -}}
{{- end -}}
{{- $kafka := $budget.kafka -}}
{{- $aggregate := int64 $kafka.metadataReserveBytes -}}
{{- range $topic := $kafka.topics -}}
{{- $aggregate = add $aggregate (mul (int64 $topic.partitions) (add (int64 $topic.budgetPerPartitionBytes) (mul (int64 $kafka.segmentBytes) (int64 $kafka.overshootSegmentsPerPartition)))) -}}
{{- end -}}
{{- if gt $aggregate (mul (int64 $budget.writersGiB.kafka) $gib) -}}
{{- fail "BLOCKED: Kafka aggregate topic/partition plus segment/internal/metadata budget exceeds Kafka filesystem allocation" -}}
{{- end -}}
{{- $writers := dict "postgres" 40 "kafka" 20 "checkpoints" 12 "scratch" 8 "metrics" 1 "k3s" 16 -}}
{{- if or (ne $budget.mode "offline-hold-separate-filesystems-required") (ne (len $budget.writersGiB) (len $writers)) -}}
{{- fail "BLOCKED: offline storage enforcement layout drift" -}}
{{- end -}}
{{- range $name, $size := $writers -}}
{{- if ne (int64 (index $budget.writersGiB $name)) (int64 $size) -}}
{{- fail "BLOCKED: offline storage enforcement layout differs from fixed partition verifier" -}}
{{- end -}}
{{- end -}}
{{- $topics := dict "ton.pool_state.v1" (mul 2 $gib) "ton.swap_events.v1" (mul 2 $gib) "__consumer_offsets" (mul 4 $gib) "__transaction_state" (mul 4 $gib) -}}
{{- $seen := dict -}}
{{- if or (ne (len $kafka.topics) 4) (ne (int64 $kafka.segmentBytes) (int64 134217728)) (ne (int64 $kafka.overshootSegmentsPerPartition) 2) (ne (int64 $kafka.metadataReserveBytes) (mul 2 $gib)) -}}
{{- fail "BLOCKED: Kafka topology/segment/metadata contract drift" -}}
{{- end -}}
{{- range $topic := $kafka.topics -}}
{{- if or (not (hasKey $topics $topic.name)) (hasKey $seen $topic.name) (ne (int64 $topic.partitions) 1) (ne (int64 $topic.budgetPerPartitionBytes) (int64 (index $topics $topic.name))) -}}
{{- fail "BLOCKED: Kafka topic/partition contract drift; exactly two data and two internal single-partition topics are budgeted" -}}
{{- end -}}
{{- $_ := set $seen $topic.name true -}}
{{- end -}}
{{- end -}}
