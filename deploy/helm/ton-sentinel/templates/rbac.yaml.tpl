{{- include "sentinel.admission" . -}}
{{- if .Values.enabled }}
apiVersion: v1
kind: Namespace
metadata:
  name: {{ include "sentinel.namespace" . }}
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/enforce-version: v1.34
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ include "sentinel.name" . }}
  namespace: {{ include "sentinel.namespace" . }}
automountServiceAccountToken: false
# No Role/Binding: applications do not administer Kubernetes.
{{- end }}
