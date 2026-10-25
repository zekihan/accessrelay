{{- define "accessrelay.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "accessrelay.fullname" -}}
{{- default .Release.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "accessrelay.selector" -}}
app.kubernetes.io/name: {{ include "accessrelay.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- with .Values.selectorLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}
{{- define "accessrelay.labels" -}}
{{ include "accessrelay.selector" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
{{- end -}}
{{- define "accessrelay.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- if .Values.image.digest -}}{{- printf "@%s" .Values.image.digest -}}{{- end -}}
{{- end -}}
