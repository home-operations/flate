{{- define "common.configmap" -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}
data:
  library: before
  value: {{ .Values.value }}
{{- end -}}
