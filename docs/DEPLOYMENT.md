# Deployment Guide — GCP Infrastructure

## Prerequisites Checklist

- [ ] GCP project with billing enabled
- [ ] `gcloud` CLI installed and authenticated
- [ ] Terraform >= 1.5 installed
- [ ] Databricks trial workspace created at https://accounts.gcp.databricks.com
- [ ] Python >= 3.10 (for data generator)

---

## Step 1: Configure GCP Project

```bash
# Set your project
export PROJECT_ID="your-actual-project-id"
gcloud config set project $PROJECT_ID

# Enable APIs (run once)
gcloud services enable \
  compute.googleapis.com \
  storage.googleapis.com \
  pubsub.googleapis.com \
  sqladmin.googleapis.com \
  servicenetworking.googleapis.com \
  cloudresourcemanager.googleapis.com \
  iam.googleapis.com

# Authenticate for Terraform
gcloud auth application-default login
```

## Step 2: Create Terraform State Bucket

```bash
gcloud storage buckets create gs://${PROJECT_ID}-tfstate \
  --location=europe-west2 \
  --uniform-bucket-level-access
```

## Step 3: Update Configuration

Edit `terraform/environments/dev/terraform.tfvars`:
```hcl
project_id = "your-actual-project-id"   # ← Replace this
region     = "europe-west2"
env        = "dev"
```

Uncomment the backend block in `terraform/environments/dev/main.tf` and set your bucket:
```hcl
backend "gcs" {
  bucket = "your-actual-project-id-tfstate"
  prefix = "terraform/state"
}
```

## Step 4: Deploy GCP Infrastructure (without Databricks module first)

The Databricks provider needs the workspace to exist first. Deploy GCP infra in two phases.

### Phase A: GCP resources only

Comment out the `module "databricks"` block in main.tf temporarily, then:

```bash
cd terraform/environments/dev

terraform init
terraform plan -var="db_password=YourSecurePass123!"
terraform apply -var="db_password=YourSecurePass123!"
```

This creates:
- VPC + subnets + NAT + firewall + Private Service Connect
- GCS buckets (landing zone, lakehouse, checkpoints) — all in europe-west2
- IAM service accounts (Databricks SA, data generator SA)
- Pub/Sub topics + subscriptions (clickstream, transactions)
- Cloud SQL PostgreSQL instance (private IP, inside VPC)

### Phase B: Databricks catalog setup

After Phase A succeeds, set Databricks credentials and uncomment the databricks module:

```bash
# Get your Databricks workspace URL from the Databricks console
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"

# Generate a personal access token in Databricks UI:
# Settings → Developer → Access tokens → Generate new token
export DATABRICKS_TOKEN="dapi_xxxxxxxxxxxxx"

# Now apply with Databricks module uncommented
terraform plan -var="db_password=YourSecurePass123!"
terraform apply -var="db_password=YourSecurePass123!"
```

This creates:
- Unity Catalog: ecommerce_dev catalog
- Schemas: bronze, silver, gold, governance
- Storage credential + external location (GCS)
- Groups: analysts, pii_readers
- Grants on gold schema

## Step 5: Seed Cloud SQL

Get the Cloud SQL private IP from Terraform output, then connect via Cloud SQL Auth Proxy:

```bash
# Install Cloud SQL Auth Proxy
# https://cloud.google.com/sql/docs/postgres/connect-auth-proxy

# Start proxy (in a separate terminal)
cloud-sql-proxy ${PROJECT_ID}:europe-west2:ecommerce-operational-dev

# In another terminal, connect and seed
psql "host=127.0.0.1 port=5432 dbname=ecommerce user=ecommerce_app password=YourSecurePass123!" \
  -f data-generator/seed_cloudsql.sql
```

## Step 6: Create Databricks Secret Scope

In your Databricks workspace:

```python
# Run in a Databricks notebook
dbutils.secrets.createScope("ecommerce")
dbutils.secrets.put("ecommerce", "cloudsql_password", "YourSecurePass123!")
dbutils.secrets.put("ecommerce", "cloudsql_user", "ecommerce_app")
```

Or via CLI:
```bash
databricks secrets create-scope ecommerce
databricks secrets put-secret ecommerce cloudsql_password --string-value "YourSecurePass123!"
databricks secrets put-secret ecommerce cloudsql_user --string-value "ecommerce_app"
```

## Step 7: Run Data Generator

```bash
cd data-generator
pip install -r requirements.txt

# Generate batch data (uploads to GCS)
python generator.py --mode batch

# Stream events to Pub/Sub (5 minutes, 10 events/sec)
python generator.py --mode stream --stream-duration 300 --stream-rate 10
```

## Step 8: Deploy DLT Pipeline

In Databricks UI:
1. Go to **Workflows → Delta Live Tables → Create pipeline**
2. Settings:
   - Name: `ecommerce-medallion`
   - Source: Git repo path or upload `databricks/notebooks/dlt/ecommerce_pipeline.py`
   - Target schema: leave blank (DLT creates tables in the schemas defined in the notebook)
   - Storage: `gs://YOUR_PROJECT_ID-lakehouse-dev/dlt/`
   - Cluster: Single node, `n2-standard-4` (trial-friendly)
3. Click **Start**

## Step 9: Run Governance Setup

After DLT pipeline completes at least one run:
1. Import `databricks/notebooks/governance/setup_governance.py` into workspace
2. Run all cells — this applies tags, creates masking views, validates locality

## Step 10: Schedule Workflow

Import the workflow definition:
```bash
databricks jobs create --json @databricks/workflows/ecommerce_workflow.json
```

Or create manually in Databricks UI: Workflows → Create Job → add tasks matching the JSON.

---

## Verification Checklist

After deployment, verify:

```bash
# GCS buckets exist and are in correct region
gcloud storage buckets describe gs://${PROJECT_ID}-landing-dev --format="value(location)"
# Should output: EUROPE-WEST2

# Pub/Sub topics
gcloud pubsub topics list --filter="name:ecommerce"

# Cloud SQL instance
gcloud sql instances describe ecommerce-operational-dev --format="value(ipAddresses)"

# Terraform state
terraform output
```

In Databricks:
```sql
-- Verify catalog structure
SHOW SCHEMAS IN ecommerce_dev;
-- Should show: bronze, silver, gold, governance

-- Verify tables after DLT run
SHOW TABLES IN ecommerce_dev.bronze;
SHOW TABLES IN ecommerce_dev.gold;

-- Verify tags
SELECT * FROM system.information_schema.table_tags WHERE catalog_name = 'ecommerce_dev';
```

---

## Cost Estimate (Trial-Friendly)

| Resource | Estimated Monthly Cost |
|---|---|
| Cloud SQL (db-f1-micro) | ~$7/month |
| GCS (few GB) | ~$0.02/month |
| Pub/Sub (low volume) | Free tier covers it |
| VPC/NAT | ~$1/month |
| Databricks (trial) | Free for 14 days |
| **Total** | **~$8/month after trial** |

---

## Troubleshooting

**"Service networking not enabled"**
```bash
gcloud services enable servicenetworking.googleapis.com
```

**Cloud SQL private IP not accessible**
- Ensure the VPC peering completed: check `google_service_networking_connection` in Terraform output
- Cloud SQL Auth Proxy is the easiest way to connect from local machine

**Databricks can't access GCS**
- Verify the storage credential in Unity Catalog has the GCS bucket IAM binding
- In GCS bucket permissions, add the Databricks service account with `Storage Object Admin`

**DLT pipeline fails on Pub/Sub read**
- Ensure `spark-pubsub` library is installed on the cluster
- Add to DLT pipeline settings: `spark.jars.packages: com.google.cloud:pubsub-spark-connector_2.12:1.0.0`
