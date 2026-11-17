{{- if .Values.enabled }}
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "sentinel.name" . }}-default-deny
  namespace: {{ include "sentinel.namespace" . }}
spec:
  podSelector: {}
  policyTypes: [Ingress, Egress]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "sentinel.name" . }}-dns
  namespace: {{ include "sentinel.namespace" . }}
spec:
  podSelector: {matchLabels: {app.kubernetes.io/instance: {{ .Release.Name }}}}
  policyTypes: [Egress]
  egress:
    - to:
        - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: kube-system}}
          podSelector: {matchLabels: {k8s-app: kube-dns}}
      ports: [{protocol: UDP, port: 53}, {protocol: TCP, port: 53}]
{{- $root := . }}
{{- range $target, $port := dict "postgres" 5432 "kafka" 9092 }}
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "sentinel.name" $root }}-{{ $target }}-ingress
  namespace: {{ include "sentinel.namespace" $root }}
spec:
  podSelector: {matchLabels: {app.kubernetes.io/instance: {{ $root.Release.Name }}, app.kubernetes.io/component: {{ $target }}}}
  policyTypes: [Ingress]
  ingress:
    - from:
        - podSelector:
            matchLabels: {app.kubernetes.io/instance: {{ $root.Release.Name }}, sentinel-db-client: "true"}
      ports: [{protocol: TCP, port: {{ $port }}}]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: {{ include "sentinel.name" $root }}-{{ $target }}-egress
  namespace: {{ include "sentinel.namespace" $root }}
spec:
  podSelector: {matchLabels: {app.kubernetes.io/instance: {{ $root.Release.Name }}, sentinel-db-client: "true"}}
  policyTypes: [Egress]
  egress:
    - to:
        - podSelector: {matchLabels: {app.kubernetes.io/instance: {{ $root.Release.Name }}, app.kubernetes.io/component: {{ $target }}}}
      ports: [{protocol: TCP, port: {{ $port }}}]
{{- end }}
# No external egress: source/TLS/private proxy remains a G3/G5 hold.
# API has no incoming route by default.
{{- end }}
