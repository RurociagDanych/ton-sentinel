{{- if .Values.enabled }}
apiVersion: batch/v1
kind: CronJob
metadata:
  name: {{ include "sentinel.name" . }}-maintenance
  namespace: {{ include "sentinel.namespace" . }}
spec:
  schedule: "17 3 * * *"
  timeZone: Etc/UTC
  suspend: true
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 1
  failedJobsHistoryLimit: 1
  startingDeadlineSeconds: 300
  jobTemplate:
    spec:
      backoffLimit: 0
      activeDeadlineSeconds: 600
      ttlSecondsAfterFinished: 3600
      template:
        metadata:
          labels: {app.kubernetes.io/instance: {{ .Release.Name }}, app.kubernetes.io/component: maintenance, sentinel-db-client: "true"}
        spec:
          serviceAccountName: {{ include "sentinel.name" . }}
          automountServiceAccountToken: false
          restartPolicy: Never
          nodeSelector:
            kubernetes.io/hostname: {{ .Values.admission.nodeName | quote }}
            kubernetes.io/arch: {{ include "sentinel.arch" . }}
          securityContext: {runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, seccompProfile: {type: RuntimeDefault}}
          containers:
            - name: maintenance
              image: {{ .Values.images.app | quote }}
              imagePullPolicy: Never
              # No invented destructive retention entrypoint; owning task must wire it.
              command: ["/bin/false"]
              securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}
              resources:
                requests: {cpu: 100m, memory: 256Mi, ephemeral-storage: 64Mi}
                limits: {cpu: 500m, memory: 512Mi, ephemeral-storage: 256Mi}
{{- end }}
