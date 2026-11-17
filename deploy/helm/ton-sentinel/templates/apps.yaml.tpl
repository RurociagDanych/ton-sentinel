{{- if .Values.enabled }}
{{- $root := . }}
{{- range $name := list "ingestor" "api" "notifier" "spark" }}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "sentinel.name" $root }}-{{ $name }}
  namespace: {{ include "sentinel.namespace" $root }}
spec:
  # Intentionally held independently of stateful replicas: no qualified app entrypoints.
  # Local K3s only: the bounded probe entrypoints below are admitted (1 replica).
  replicas: {{ if include "sentinel.localK3s" $root }}1{{ else }}0{{ end }}
  strategy: {type: Recreate}
  selector:
    matchLabels: {app.kubernetes.io/instance: {{ $root.Release.Name }}, app.kubernetes.io/component: {{ $name }}}
  template:
    metadata:
      labels: {app.kubernetes.io/instance: {{ $root.Release.Name }}, app.kubernetes.io/component: {{ $name }}, sentinel-db-client: "true"}
    spec:
      serviceAccountName: {{ include "sentinel.name" $root }}
      automountServiceAccountToken: false
      nodeSelector: {kubernetes.io/hostname: {{ $root.Values.admission.nodeName | quote }}, kubernetes.io/arch: {{ include "sentinel.arch" $root }}}
      securityContext: {runAsNonRoot: true, runAsUser: {{ if and (eq $name "spark") (include "sentinel.localK3s" $root) }}185{{ else }}10001{{ end }}, runAsGroup: {{ if and (eq $name "spark") (include "sentinel.localK3s" $root) }}185{{ else }}10001{{ end }}, fsGroup: 10001, seccompProfile: {type: RuntimeDefault}}
      # Local K3s spark: the pinned spark image's own non-root uid (185) has a passwd entry; Hadoop's login fails for uid 10001.
      containers:
        - name: {{ $name }}
          image: {{ if eq $name "spark" }}{{ $root.Values.images.spark | quote }}{{ else }}{{ $root.Values.images.app | quote }}{{ end }}
          imagePullPolicy: Never
          # Explicit refusal, NOT a fabricated Python module or Spark submit script.
          # Replacement and readiness probes belong to application/ARM qualification.
          {{- if include "sentinel.localK3s" $root }}
          # Local K3s only: bounded probe over existing project code (ton_sentinel.k3s_probe). The source is
          # delivered by the test-created ConfigMap `<name>-code` (the pinned app image does not contain src/).
          command: ["python3", "-c", "import sys,tarfile; tarfile.open('/code/src.tgz').extractall('/tmp/code'); sys.path.insert(0,'/tmp/code/src'); from ton_sentinel.k3s_probe import main; sys.exit(main(sys.argv[1:]))", {{ $name | quote }}]
          readinessProbe: {exec: {command: ["test", "-f", "/tmp/ready"]}, periodSeconds: 3, failureThreshold: 200}
          {{- else }}
          command: ["/bin/false"]
          {{- end }}
          securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}
          resources:
            requests:
              cpu: {{ if eq $name "spark" }}500m{{ else }}100m{{ end }}
              memory: {{ if eq $name "spark" }}1024Mi{{ else }}256Mi{{ end }}
              ephemeral-storage: 128Mi
            limits:
              cpu: {{ if eq $name "spark" }}"1"{{ else }}300m{{ end }}
              memory: {{ if eq $name "spark" }}1536Mi{{ else }}512Mi{{ end }}
              ephemeral-storage: 512Mi
          env:
            - name: SENTINEL_DATABASE_DSN
              valueFrom: {secretKeyRef: {name: {{ $root.Values.existingSecret }}, key: database-dsn}}
            - name: SENTINEL_SOURCE_API_KEY
              valueFrom: {secretKeyRef: {name: {{ $root.Values.existingSecret }}, key: source-api-key}}
            {{- if include "sentinel.localK3s" $root }}
            - {name: HOME, value: /tmp}
            - {name: PYTHONDONTWRITEBYTECODE, value: "1"}
            - {name: HADOOP_USER_NAME, value: sentinel}
            - {name: SENTINEL_KAFKA_BOOTSTRAP, value: "{{ include "sentinel.name" $root }}-kafka:9092"}
            {{- if eq $name "notifier" }}
            - {name: SENTINEL_KAFKA_TLS_DIR, value: /kafka-tls}
            {{- end }}
            {{- if eq $name "spark" }}
            - {name: PYTHONPATH, value: "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip"}
            {{- end }}
            {{- end }}
            {{- if eq $name "spark" }}
            - {name: SPARK_DRIVER_MEMORY, value: 512m}
            - {name: SPARK_EXECUTOR_MEMORY, value: 512m}
            - {name: SPARK_LOCAL_DIRS, value: /var/sentinel/scratch}
            {{- end }}
          volumeMounts:
            - {name: tmp, mountPath: /tmp}
            {{- if include "sentinel.localK3s" $root }}
            - {name: code, mountPath: /code, readOnly: true}
            {{- if eq $name "notifier" }}
            - {name: kafka-tls, mountPath: /kafka-tls, readOnly: true}
            {{- end }}
            {{- end }}
            {{- if eq $name "spark" }}
            - {name: checkpoints, mountPath: /var/sentinel/checkpoints}
            - {name: scratch, mountPath: /var/sentinel/scratch}
            {{- end }}
      volumes:
        - name: tmp
          emptyDir: {sizeLimit: 256Mi}
        {{- if include "sentinel.localK3s" $root }}
        - name: code
          configMap: {name: {{ include "sentinel.name" $root }}-code}
        {{- if eq $name "notifier" }}
        - name: kafka-tls
          secret:
            secretName: {{ $root.Values.existingSecret }}
            items:
              - {key: kafka-client.crt, path: kafka-client.crt}
              - {key: kafka-client.key, path: kafka-client.key}
              - {key: kafka-ca.crt, path: kafka-ca.crt}
        {{- end }}
        {{- end }}
        {{- if eq $name "spark" }}
        - name: checkpoints
          persistentVolumeClaim: {claimName: {{ include "sentinel.name" $root }}-checkpoints}
        - name: scratch
          persistentVolumeClaim: {claimName: {{ include "sentinel.name" $root }}-scratch}
        {{- end }}
{{- end }}
{{- end }}
