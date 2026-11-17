{{- if .Values.enabled }}
apiVersion: v1
kind: ResourceQuota
metadata:
  name: {{ include "sentinel.name" . }}
  namespace: {{ include "sentinel.namespace" . }}
spec:
  hard:
    requests.cpu: "1800m"
    requests.memory: 4608Mi
    limits.cpu: "5"
    limits.memory: 6144Mi
    requests.ephemeral-storage: 2Gi
    limits.ephemeral-storage: 4Gi
    requests.storage: 81Gi
    persistentvolumeclaims: "5"
    pods: "7"
    services.loadbalancers: "0"
    services.nodeports: "0"
{{- end }}
