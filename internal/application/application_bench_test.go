package application_test

import (
	"testing"

	"github.com/home-operations/flate/internal/application"
)

func BenchmarkContains_Workload(b *testing.B) {
	content := []byte(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: apps
  labels:
    app.kubernetes.io/name: web
spec:
  replicas: 2
  selector:
    matchLabels:
      app.kubernetes.io/name: web
  template:
    metadata:
      labels:
        app.kubernetes.io/name: web
    spec:
      serviceAccountName: web
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        runAsGroup: 1000
        fsGroup: 1000
      containers:
        - name: web
          image: ghcr.io/example/web:1.0.0
          ports:
            - name: http
              containerPort: 8080
          envFrom:
            - configMapRef:
                name: web-config
            - secretRef:
                name: web-secrets
          readinessProbe:
            httpGet:
              path: /ready
              port: http
          livenessProbe:
            httpGet:
              path: /health
              port: http
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              memory: 256Mi
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            capabilities:
              drop: [ALL]
          volumeMounts:
            - name: tmp
              mountPath: /tmp
      volumes:
        - name: tmp
          emptyDir: {}
`)
	b.ReportAllocs()
	b.SetBytes(int64(len(content)))
	for b.Loop() {
		if application.Contains(content) {
			b.Fatal("workload cannot produce an entry application")
		}
	}
}
