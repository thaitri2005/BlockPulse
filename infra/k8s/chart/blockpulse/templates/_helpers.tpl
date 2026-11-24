{{/* Names are prefixed with the release so two installs can share a namespace. */}}
{{- define "blockpulse.name" -}}
{{- printf "%s-%s" .root.Release.Name .component | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels on every object: who owns it and which part of the stack it is. */}}
{{- define "blockpulse.labels" -}}
app.kubernetes.io/name: blockpulse
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .root.Chart.Name .root.Chart.Version }}
{{- end -}}

{{/* The subset that never changes, used to match pods to their Service and StatefulSet. */}}
{{- define "blockpulse.selector" -}}
app.kubernetes.io/name: blockpulse
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* In-cluster addresses, defined once so every workload agrees on them. */}}
{{- define "blockpulse.kafkaBootstrap" -}}
{{- $name := include "blockpulse.name" (dict "root" . "component" "kafka") -}}
{{- printf "%s-0.%s:9092" $name $name -}}
{{- end -}}

{{/* Settings shared by every container that runs the BlockPulse image. */}}
{{- define "blockpulse.appContainer" -}}
image: {{ .Values.app.image }}
imagePullPolicy: {{ .Values.app.pullPolicy }}
securityContext:
  runAsNonRoot: true
  allowPrivilegeEscalation: false
  capabilities: {drop: ["ALL"]}
{{- end -}}
