The different deployment options in GCP, how we can integrate Kubernetes:

1. Traditional VM Deployment (Google Compute Engine):

```
Pipeline Components:
- VM 1: Data Pipeline (stocks_pipeline.py + bigquery_loader.py)
- VM 2: Dashboard Server (dashboard.py)

Pros:
- Simple to set up and manage
- Direct control over resources
- Easy to SSH and debug

Cons:
- Less scalable
- Manual maintenance required
- Not containerized
```

2. Container-Based Deployment (Cloud Run):

```
Better Approach:
- Deploy pipeline container to Cloud Run
- Deploy dashboard container to Cloud Run

Pros:
- Fully managed
- Auto-scaling
- Pay per use
- Easy deployment with Dockerfile
- Simple HTTPS endpoints
```

Let me show you both approaches:

```yaml
# cloudbuild.yaml
steps:
  # Build pipeline image
  - name: "gcr.io/cloud-builders/docker"
    args:
      [
        "build",
        "-t",
        "gcr.io/$PROJECT_ID/stock-pipeline",
        "-f",
        "Dockerfile.pipeline",
        ".",
      ]

  # Build dashboard image
  - name: "gcr.io/cloud-builders/docker"
    args:
      [
        "build",
        "-t",
        "gcr.io/$PROJECT_ID/stock-dashboard",
        "-f",
        "Dockerfile.dashboard",
        ".",
      ]

  # Push images to Container Registry
  - name: "gcr.io/cloud-builders/docker"
    args: ["push", "gcr.io/$PROJECT_ID/stock-pipeline"]
  - name: "gcr.io/cloud-builders/docker"
    args: ["push", "gcr.io/$PROJECT_ID/stock-dashboard"]

  # Deploy to Cloud Run
  - name: "gcr.io/cloud-builders/gcloud"
    args:
      - "run"
      - "deploy"
      - "stock-pipeline"
      - "--image"
      - "gcr.io/$PROJECT_ID/stock-pipeline"
      - "--region"
      - "us-central1"
      - "--platform"
      - "managed"

  - name: "gcr.io/cloud-builders/gcloud"
    args:
      - "run"
      - "deploy"
      - "stock-dashboard"
      - "--image"
      - "gcr.io/$PROJECT_ID/stock-dashboard"
      - "--region"
      - "us-central1"
      - "--platform"
      - "managed"
      - "--allow-unauthenticated"

images:
  - "gcr.io/$PROJECT_ID/stock-pipeline"
  - "gcr.io/$PROJECT_ID/stock-dashboard"
```

```dockerfile
# Dockerfile.pipeline
FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY stocks_pipeline.py .
COPY bigquery_loader.py .
COPY config.py .

# Copy service account key
COPY keys/service_account.json /app/keys/

# Set environment variable for Google Cloud credentials
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/keys/service_account.json"

# Run the pipeline
CMD ["python", "stocks_pipeline.py"]

```

```dockerfile
# Dockerfile.dashboard
FROM python:3.9-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY dashboard.py .
COPY config.py .

# Copy service account key
COPY keys/service_account.json /app/keys/

# Set environment variable for Google Cloud credentials
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/keys/service_account.json"

# Expose port for Streamlit
EXPOSE 8501

# Run the dashboard
CMD ["streamlit", "run", "dashboard.py", "--server.port", "8501", "--server.address", "0.0.0.0"]

```

Now, for Kubernetes deployment, here's how we can set it up:

```yaml
# kubernetes/pipeline-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stock-pipeline
spec:
  replicas: 1
  selector:
    matchLabels:
      app: stock-pipeline
  template:
    metadata:
      labels:
        app: stock-pipeline
    spec:
      containers:
        - name: stock-pipeline
          image: gcr.io/stock-data-pipeline-444011/stock-pipeline
          env:
            - name: GOOGLE_APPLICATION_CREDENTIALS
              value: "/app/keys/service_account.json"
          volumeMounts:
            - name: google-cloud-key
              mountPath: /app/keys
      volumes:
        - name: google-cloud-key
          secret:
            secretName: gcp-key
---
# kubernetes/dashboard-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stock-dashboard
spec:
  replicas: 2
  selector:
    matchLabels:
      app: stock-dashboard
  template:
    metadata:
      labels:
        app: stock-dashboard
    spec:
      containers:
        - name: stock-dashboard
          image: gcr.io/stock-data-pipeline-444011/stock-dashboard
          ports:
            - containerPort: 8501
          env:
            - name: GOOGLE_APPLICATION_CREDENTIALS
              value: "/app/keys/service_account.json"
          volumeMounts:
            - name: google-cloud-key
              mountPath: /app/keys
      volumes:
        - name: google-cloud-key
          secret:
            secretName: gcp-key
---
# kubernetes/dashboard-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: stock-dashboard
spec:
  type: LoadBalancer
  ports:
    - port: 80
      targetPort: 8501
  selector:
    app: stock-dashboard
```

Deployment Steps:

1. Cloud Run Deployment:

```bash
# Build and deploy using Cloud Build
gcloud builds submit

# The dashboard will be available at the provided Cloud Run URL
```

2. Kubernetes Deployment (GKE):

```bash
# Create GKE cluster
gcloud container clusters create stock-cluster \
    --num-nodes=3 \
    --zone=us-central1-a

# Create secret for service account
kubectl create secret generic gcp-key \
    --from-file=service_account.json=./keys/service_account.json

# Apply Kubernetes configurations
kubectl apply -f kubernetes/pipeline-deployment.yaml
kubectl apply -f kubernetes/dashboard-deployment.yaml
kubectl apply -f kubernetes/dashboard-service.yaml

# Get dashboard URL
kubectl get service stock-dashboard
```

Benefits of Kubernetes:

1. Scalability

   - Auto-scaling based on load
   - Easy horizontal scaling
   - Resource optimization

2. High Availability

   - Multiple replicas
   - Auto-healing
   - Load balancing

3. Management

   - Rolling updates
   - Easy rollbacks
   - Health monitoring
   - Resource management

4. Development
   - Consistent environments
   - Easy CI/CD integration
   - Development/Production parity

The Streamlit dashboard will be publicly accessible! The exact URL/IP depends on how you deploy it:

1. If using Cloud Run:

- You'll get a public HTTPS URL automatically like:
  `https://stock-dashboard-xxxxx-uc.a.run.app`
- This URL is provided after deployment
- Already secure with HTTPS
- No need to manage IP addresses

2. If using GKE (Kubernetes):

- The LoadBalancer service will provide an external IP
- Get it using:

```bash
kubectl get service stock-dashboard
```

Example output:

```
NAME             TYPE           CLUSTER-IP      EXTERNAL-IP      PORT(S)        AGE
stock-dashboard  LoadBalancer   10.x.x.x        34.x.x.x         80:30000/TCP   1m
```

- Access dashboard at: `http://34.x.x.x` (EXTERNAL-IP)
- You should add HTTPS using Cloud Load Balancer or Ingress

3. Security Considerations:

- Add authentication if needed
- Restrict access using IAM
- Add SSL/TLS for Kubernetes deployment
- Consider using Cloud IAP (Identity-Aware Proxy)
