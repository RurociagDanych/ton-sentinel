{{- if .Values.enabled }}
{{- include "sentinel.admission" . -}}
{{- $root := . -}}
{{- $kafkaBudget := (mustFromJson (.Files.Get "storage-budget.json")).kafka -}}
{{- $dataRetention := int64 0 -}}
{{- range $topic := $kafkaBudget.topics -}}
{{- if eq $topic.name "ton.pool_state.v1" -}}
{{- $dataRetention = int64 $topic.budgetPerPartitionBytes -}}
{{- end -}}
{{- end -}}
{{- range $name, $port := dict "postgres" 5432 "kafka" 9092 }}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: {{ include "sentinel.name" $root }}-{{ $name }}
  namespace: {{ include "sentinel.namespace" $root }}
spec:
  replicas: {{ $root.Values.replicas }}
  serviceName: {{ include "sentinel.name" $root }}-{{ $name }}
  selector:
    matchLabels: {app.kubernetes.io/instance: {{ $root.Release.Name }}, app.kubernetes.io/component: {{ $name }}}
  template:
    metadata:
      labels: {app.kubernetes.io/instance: {{ $root.Release.Name }}, app.kubernetes.io/component: {{ $name }}}
    spec:
      serviceAccountName: {{ include "sentinel.name" $root }}
      automountServiceAccountToken: false
      nodeSelector: {kubernetes.io/hostname: {{ $root.Values.admission.nodeName | quote }}, kubernetes.io/arch: {{ include "sentinel.arch" $root }}}
      terminationGracePeriodSeconds: 90
      securityContext:
        runAsNonRoot: true
        runAsUser: {{ if eq $name "postgres" }}999{{ else }}1000{{ end }}
        runAsGroup: {{ if eq $name "postgres" }}999{{ else }}1000{{ end }}
        fsGroup: {{ if eq $name "postgres" }}999{{ else }}1000{{ end }}
        seccompProfile: {type: RuntimeDefault}
      containers:
        - name: {{ $name }}
          image: {{ index $root.Values.images $name | quote }}
          imagePullPolicy: Never
          securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}
          ports: [{containerPort: {{ $port }}, name: service}]
          resources:
            requests:
              cpu: {{ if eq $name "postgres" }}300m{{ else }}500m{{ end }}
              memory: {{ if eq $name "postgres" }}1024Mi{{ else }}1536Mi{{ end }}
              ephemeral-storage: 256Mi
            limits:
              cpu: "1"
              memory: {{ if eq $name "postgres" }}1024Mi{{ else }}1536Mi{{ end }}
              ephemeral-storage: 512Mi
          env:
            {{- if eq $name "postgres" }}
            - {name: PGDATA, value: /var/lib/postgresql/data/pgdata}
            - {name: POSTGRES_DB, value: sentinel}
            - {name: POSTGRES_USER, value: sentinel}
            - name: POSTGRES_PASSWORD
              valueFrom: {secretKeyRef: {name: {{ $root.Values.existingSecret }}, key: postgres-password}}
          args: ["-c", "shared_buffers=128MB", "-c", "max_connections=30", "-c", "wal_keep_size=256MB", "-c", "max_wal_size=512MB", "-c", "min_wal_size=128MB", "-c", "temp_file_limit=512MB", "-c", "log_statement=none", "-c", "ssl=on", "-c", "ssl_cert_file=/tls/postgres.crt", "-c", "ssl_key_file=/tls/postgres.key"]
            {{- else }}
            - {name: KAFKA_NODE_ID, value: "1"}
            - {name: CLUSTER_ID, value: "c2VudGluZWxvZmZsaW5lMQ"}
            - {name: KAFKA_PROCESS_ROLES, value: "broker,controller"}
            - {name: KAFKA_CONTROLLER_QUORUM_VOTERS, value: "1@localhost:9093"}
            - {name: KAFKA_LISTENERS, value: "SSL://:9092,CONTROLLER://localhost:9093"}
            - {name: KAFKA_ADVERTISED_LISTENERS, value: "SSL://{{ include "sentinel.name" $root }}-kafka:9092"}
            - {name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, value: "SSL:SSL,CONTROLLER:PLAINTEXT"}
            - {name: KAFKA_INTER_BROKER_LISTENER_NAME, value: "SSL"}
            - {name: KAFKA_CONTROLLER_LISTENER_NAMES, value: "CONTROLLER"}
            - {name: KAFKA_SSL_KEYSTORE_LOCATION, value: "/tls/kafka.keystore.jks"}
            - {name: KAFKA_SSL_TRUSTSTORE_LOCATION, value: "/tls/kafka.truststore.jks"}
            - {name: KAFKA_SSL_CLIENT_AUTH, value: "required"}
            - name: KAFKA_SSL_KEYSTORE_PASSWORD
              valueFrom: {secretKeyRef: {name: {{ $root.Values.existingSecret }}, key: kafka-keystore-password}}
            - name: KAFKA_SSL_TRUSTSTORE_PASSWORD
              valueFrom: {secretKeyRef: {name: {{ $root.Values.existingSecret }}, key: kafka-truststore-password}}
            {{- if include "sentinel.localK3s" $root }}
            # Local K3s only: apache/kafka's configure script reads TLS material from /etc/kafka/secrets.
            - {name: KAFKA_SSL_KEYSTORE_FILENAME, value: kafka.keystore.jks}
            - {name: KAFKA_SSL_TRUSTSTORE_FILENAME, value: kafka.truststore.jks}
            - {name: LOG_DIR, value: /tmp/kafka-logs}
            - {name: KAFKA_SSL_KEY_CREDENTIALS, value: kafka-credentials}
            - {name: KAFKA_SSL_KEYSTORE_CREDENTIALS, value: kafka-credentials}
            - {name: KAFKA_SSL_TRUSTSTORE_CREDENTIALS, value: kafka-credentials}
            {{- end }}
            - {name: KAFKA_LOG_DIRS, value: /var/lib/kafka/data}
            - {name: KAFKA_HEAP_OPTS, value: "-Xms256m -Xmx512m"}
            - {name: KAFKA_LOG_RETENTION_BYTES, value: {{ $dataRetention | quote }}}
            - {name: KAFKA_LOG_SEGMENT_BYTES, value: {{ $kafkaBudget.segmentBytes | int64 | quote }}}
            - {name: KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS, value: "1"}
            - {name: KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS, value: "1"}
            - {name: KAFKA_OFFSETS_TOPIC_SEGMENT_BYTES, value: {{ $kafkaBudget.segmentBytes | int64 | quote }}}
            - {name: KAFKA_TRANSACTION_STATE_LOG_SEGMENT_BYTES, value: {{ $kafkaBudget.segmentBytes | int64 | quote }}}
            - {name: KAFKA_METADATA_LOG_SEGMENT_BYTES, value: {{ $kafkaBudget.segmentBytes | int64 | quote }}}
            - {name: KAFKA_METADATA_MAX_RETENTION_BYTES, value: "1073741824"}
            - {name: KAFKA_LOG_RETENTION_HOURS, value: "24"}
            - {name: KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS, value: "60000"}
            - {name: KAFKA_AUTO_CREATE_TOPICS_ENABLE, value: "false"}
            - {name: KAFKA_NUM_PARTITIONS, value: "1"}
            - {name: KAFKA_DEFAULT_REPLICATION_FACTOR, value: "1"}
            - {name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, value: "1"}
            - {name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, value: "1"}
            - {name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR, value: "1"}
            {{- end }}
          readinessProbe:
            tcpSocket: {port: service}
            initialDelaySeconds: 30
            periodSeconds: 10
          startupProbe:
            tcpSocket: {port: service}
            failureThreshold: 30
            periodSeconds: 10
          volumeMounts:
            - name: data
              mountPath: {{ if eq $name "postgres" }}/var/lib/postgresql/data{{ else }}/var/lib/kafka/data{{ end }}
            - {name: tmp, mountPath: /tmp}
            - {name: runtime, mountPath: /var/run/postgresql}
            - {name: tls, mountPath: /tls, readOnly: true}
            {{- if and (eq $name "kafka") (include "sentinel.localK3s" $root) }}
            - {name: kafka-config, mountPath: /opt/kafka/config}
            - {name: tls, mountPath: /etc/kafka/secrets, readOnly: true}
            {{- end }}
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: {{ include "sentinel.name" $root }}-{{ $name }}}
        - name: tmp
          emptyDir: {sizeLimit: 256Mi}
        - name: runtime
          emptyDir: {sizeLimit: 16Mi}
        {{- if and (eq $name "kafka") (include "sentinel.localK3s" $root) }}
        # Local K3s only: apache/kafka's configure step needs a writable /opt/kafka/config.
        - name: kafka-config
          emptyDir: {sizeLimit: 16Mi}
        {{- end }}
        - name: tls
          secret:
            secretName: {{ $root.Values.existingSecret }}
            defaultMode: 0440
            items:
              {{- if eq $name "postgres" }}
              - {key: postgres.crt, path: postgres.crt}
              - {key: postgres.key, path: postgres.key}
              {{- else }}
              - {key: kafka.keystore.jks, path: kafka.keystore.jks}
              - {key: kafka.truststore.jks, path: kafka.truststore.jks}
              {{- if include "sentinel.localK3s" $root }}
              - {key: kafka-credentials, path: kafka-credentials}
              {{- end }}
              {{- end }}
{{- end }}
{{- end }}
