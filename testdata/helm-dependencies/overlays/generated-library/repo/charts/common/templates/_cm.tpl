{{- define "common.configmap" -}}
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ .Release.Name }}
data:
  library: after
  value: {{ .Values.value }}
{{- end -}}
