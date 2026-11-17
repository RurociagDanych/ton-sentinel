{{- if .Values.enabled }}
{{- $root := . }}
{{- range $name, $port := dict "postgres" 5432 "kafka" 9092 "api" 8000 }}
---
apiVersion: v1
kind: Service
metadata:
  name: {{ include "sentinel.name" $root }}-{{ $name }}
  namespace: {{ include "sentinel.namespace" $root }}
spec:
  type: ClusterIP
  {{- if or (eq $name "postgres") (eq $name "kafka") }}
  clusterIP: None
  {{- end }}
  selector:
    app.kubernetes.io/instance: {{ $root.Release.Name }}
    app.kubernetes.io/component: {{ $name }}
  ports:
    - name: service
      port: {{ $port }}
      targetPort: {{ $port }}
{{- end }}
{{- end }}
