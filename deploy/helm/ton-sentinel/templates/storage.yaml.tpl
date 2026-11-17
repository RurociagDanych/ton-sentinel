{{- if .Values.enabled }}
{{- include "sentinel.admission" . -}}
{{- $root := . -}}
{{- $budget := mustFromJson (.Files.Get "storage-budget.json") -}}
{{- $sizes := dict -}}
{{- range $name := list "postgres" "kafka" "checkpoints" "scratch" "metrics" -}}
{{- $_ := set $sizes $name (printf "%dGi" (int (index $budget.writersGiB $name))) -}}
{{- end -}}
{{- range $name, $capacity := $sizes }}
{{- if not (include "sentinel.localK3s" $root) }}
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: {{ include "sentinel.name" $root }}-{{ $name }}
spec:
  capacity: {storage: {{ $capacity }}}
  volumeMode: Filesystem
  accessModes: [ReadWriteOnce]
  persistentVolumeReclaimPolicy: Retain
  storageClassName: ""
  claimRef:
    name: {{ include "sentinel.name" $root }}-{{ $name }}
    namespace: {{ include "sentinel.namespace" $root }}
  local:
    path: /srv/ton-sentinel/{{ $name }}
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: kubernetes.io/hostname
              operator: In
              values: [{{ $root.Values.admission.nodeName | quote }}]
{{- end }}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: {{ include "sentinel.name" $root }}-{{ $name }}
  namespace: {{ include "sentinel.namespace" $root }}
spec:
  accessModes: [ReadWriteOnce]
  {{- if include "sentinel.localK3s" $root }}
  storageClassName: {{ $root.Values.local.storageClass | quote }}
  {{- else }}
  storageClassName: ""
  volumeName: {{ include "sentinel.name" $root }}-{{ $name }}
  {{- end }}
  resources:
    requests: {storage: {{ $capacity }}}
{{- end }}
{{- end }}
