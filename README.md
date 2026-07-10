# Supply Chain Warehouse Throughput Prediction Pipeline

A cloud-native, event-driven data pipeline built on AWS that ingests raw FMCG warehouse data, cleans it through a medallion architecture (Bronze → Silver), trains a regression model to predict warehouse product throughput, and exposes predictions via a REST API — all provisioned through Infrastructure as Code (AWS CDK).

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup & Deployment](#setup--deployment)
- [Usage](#usage)
- [Design Decisions](#design-decisions)
- [AWS Well-Architected Framework Alignment](#aws-well-architected-framework-alignment)

## Overview

This project predicts the total product weight (in tons) a warehouse is expected to handle, based on its operational and infrastructure attributes — capacity, zone, worker count, distance from hub, government certification, flood risk, and more.

**Use case:** Supply chain planners can use this to estimate throughput for new or existing warehouses, identify under/over-utilized facilities, and inform infrastructure investment decisions.

**Dataset:** [Supply Chain Optimization for a FMCG Company](https://www.kaggle.com/code/mernaassaad/sco-for-a-fmcg-company/input) (Kaggle)

## Architecture

<img width="791" height="782" alt="CSCI4149_architecture_diagram-2" src="https://github.com/user-attachments/assets/dbaba388-4c99-4009-b6b3-38d83adf590a" />

**Two-phase pipeline:**

1. **Data pipeline** — Bronze (raw CSV) → Glue ETL cleans and validates → Silver (Parquet)
2. **ML pipeline** — Silver triggers automatic model training → Model bucket → API Gateway serves live predictions from raw, human-readable warehouse attributes

The two Lambda functions (training and inference) never call each other directly — they communicate only through the Model S3 bucket, keeping the system loosely coupled. Both run the *identical* `encode_features` function so that a category seen at inference time is guaranteed to map to the same one-hot column the model was trained on; any column the model expects that isn't produced by a single-row request (e.g. a category not present in that specific request) is filled with `0` via `reindex`, correctly representing "this row is not that category."

## Tech Stack

| Component | Service | Why |
|---|---|---|
| Raw/clean storage | Amazon S3 (4 buckets: Bronze, Silver, Model, Scripts) | Cheap, durable, inherently multi-AZ; separate buckets enforce single-responsibility storage (data buckets hold pipeline data only, Scripts holds ETL code) |
| ETL | AWS Glue (Python Shell job, 1 DPU) | Dataset is a single flat CSV — Spark's distributed compute (2–100 DPU) is unnecessary overhead; Python Shell (0.0625–1 DPU) is cheaper and faster to iterate |
| Pipeline trigger | AWS Lambda | S3 event notifications can't invoke Glue directly (only Lambda, SNS, or SQS); Lambda bridges the S3 event to the Glue `StartJobRun` API |
| Model training/inference | AWS Lambda (Docker container image via Amazon ECR) | Pay-per-invocation avoids idle server costs (vs. EC2/SageMaker); container deployment (10GB limit) was required once combined ML dependencies (pandas + scikit-learn + scipy, ~356MB) exceeded Lambda's 250MB zip/layer limit |
| API exposure | Amazon API Gateway (`LambdaRestApi`, proxy integration) | Standard serverless pattern for exposing Lambda over HTTP; `POST /predict` resource explicitly defined rather than a catch-all proxy route |
| IaC | AWS CDK (Python) | Full infrastructure defined in code; more readable and maintainable than raw CloudFormation YAML, and generates CloudFormation under the hood |
| Monitoring | Amazon CloudWatch | Automatic logging for all Lambda/Glue executions; CloudWatch Alarms configured on Lambda error metrics (`metric_errors()`) and Glue job failed-task metrics |

**SageMaker** was originally considered for model hosting but was unavailable due to AWS Academy Learner Lab IAM restrictions (unable to create a SageMaker execution role via the console or CDK). **EC2 + Flask** was also considered as a fallback, but rejected in favor of Lambda: EC2 bills hourly regardless of traffic, whereas Lambda's pay-per-invocation model better matches the pipeline's low, bursty inference load, and avoids server/OS management entirely.

## Prerequisites

- Python 3.9+
- Node.js 18+ (required for the CDK CLI)
- [AWS CDK CLI](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html): `npm install -g aws-cdk`
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running, for building the training/inference Lambda container images)
- AWS CLI configured with credentials for a non-root IAM user: `aws configure`
- An AWS account (Academy Learner Lab sandbox or personal account) with permissions for S3, Lambda, Glue, IAM, API Gateway, and CloudWatch

## Project Structure

```
supply_chain_pipeline/
├── app.py                          # CDK app entry point
├── supply_chain_pipeline_stack.py  # All infrastructure: S3, Glue, Lambda,
│                                    # API Gateway, IAM, CloudWatch alarms
├── glue/
│   └── etl_job.py                  # Bronze → Silver cleaning script
│                                    # (dynamic Yes/No mapping, dedup, imputation)
├── src/
│   ├── lambda_trigger/
│   │   └── handler.py              # S3 event → starts Glue job
│   ├── lambda_training_container/  # Docker-based training Lambda
│   │   ├── Dockerfile              # base: public.ecr.aws/lambda/python:3.9
│   │   ├── requirements.txt        # pandas, scikit-learn, pyarrow, boto3
│   │   └── handler.py              # drop IDs → encode → train → bundle → S3
│   └── lambda_inference_container/ # Docker-based inference Lambda
│       ├── Dockerfile
│       ├── requirements.txt        # pandas, scikit-learn, boto3 (no pyarrow needed)
│       └── handler.py              # encode (identical to training) → reindex → predict
├── scripts/
│   ├── predict.py                  # Interactive local client for demo predictions
│   └── validate_predictions.py     # Samples real Silver rows, compares true vs.
│                                    # predicted via the live API
├── requirements.txt                # Root-level: CDK + local testing deps
├── cdk.json
├── .gitignore                      # excludes .venv, cdk.out, layers/, datasets, *.pkl
├── runbook.md
└── README.md
```

## Setup & Deployment

### 1. Clone and install dependencies

```bash
git clone https://github.com/anshdeepb/supply-chain-pipeline-AWS.git
cd supply_chain_pipeline
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure AWS credentials

```bash
aws configure
```

Enter your Access Key ID, Secret Access Key, region (`us-east-1` recommended), and output format (`json`). Use a dedicated IAM user — avoid root credentials.

### 3. Bootstrap CDK (one-time per AWS account)

```bash
cdk bootstrap
```

### 4. Ensure Docker Desktop is running

The training and inference Lambdas are deployed as container images (required to fit their ML dependencies within Lambda's limits), so Docker must be active before deploying.

### 5. Deploy the stack

```bash
cdk synth      # sanity check — compiles to CloudFormation, no AWS changes
cdk deploy     # provisions everything: S3, Glue, Lambda, API Gateway, CloudWatch
```

First deploy takes longer than usual due to Docker image builds and pushes to ECR.

### 6. Download the dataset

Download the CSV from the [Kaggle link above](#overview) and note its local path — you'll upload it in the next step.

### 7. Trigger the pipeline

Upload the dataset into the bronze layer

This alone kicks off the entire pipeline automatically: Glue cleans the data, the training Lambda fires on the new Silver object, and a model is trained and saved — no further manual steps required.

### 8. Verify

Check the silver bucket, which should now contain the new cleansed and transformed data file.

## Usage

Once deployed and the model has trained, get predictions using the included client script:

```bash
python scripts/predict.py
```

You'll be prompted for warehouse attributes (capacity, worker count, distance from hub, etc.) and the script returns a predicted `product_wg_ton` value via the deployed API Gateway endpoint.

**Important — send raw attributes, not pre-encoded values.** The inference Lambda replicates the exact training-time encoding pipeline (one-hot encoding via `pd.get_dummies`, followed by `reindex` against the saved `feature_columns` list) so the client can send human-readable category strings (e.g. `"zone": "North"`, `"WH_capacity_size": "Mid"`) rather than manually constructing one-hot columns.

Returns:
```json
{"predicted_product_wg_ton": 19630.84}
```

## Design Decisions

- **S3 for Bronze/Silver instead of RDS** — prioritizes read speed and cost for bulk ML workloads; a future Gold-layer RDS table is the natural extension point for concurrent analyst SQL access.
- **CSV → Parquet conversion at Silver** — columnar storage, better compression, and native type preservation compared to CSV; standard practice for medallion Silver/Gold layers.
- **Dynamic, schema-agnostic cleaning** — the Glue ETL script detects binary Yes/No columns by inspecting each column's actual unique values (rather than hardcoding names like `flood_proof`), remains numeric-column-safe via `pd.api.types.is_numeric_dtype`, and applies duplicate removal, target-null exclusion, and median imputation only where relevant.
- **Identifier columns dropped via cardinality threshold** (>95% unique values, `nunique() / len(df)`) rather than hardcoded by name (e.g. `Ware_house_ID`, `WH_Manager_ID`) — mirrors real-world practice of combining automated heuristics with explicit review, and makes the pipeline resilient to new ID-like columns appearing in future data.
- **One-hot encoding over label encoding for nominal categories** (e.g. `zone`, `wh_owner_type`) — avoids implying a false ordinal relationship between categories with no inherent order. Acknowledged tradeoff: this treats all remaining categoricals as nominal rather than distinguishing genuinely ordinal ones (e.g. `WH_capacity_size`), trading a small amount of potential model signal for a simpler, fully schema-agnostic pipeline.
- **80/20 train-test split** (`random_state=42`) — ensures RMSE/R² reflect performance on unseen data rather than an optimistic training-data score.
- **Linear Regression as the initial model** — chosen for interpretability (coefficients directly show feature impact — the strongest predictor identified was `approved_wh_govt_certificate`, plausible given certification tiers commonly gate warehouse capacity/compliance) and fast training time within Lambda's execution constraints.
- **Docker container Lambdas instead of zip + layers** — pandas + scikit-learn + scipy totaled ~356MB uncompressed (scipy alone: 103MB), exceeding Lambda's 250MB unzipped layer limit even after removing `pyarrow`. Container images (10GB limit, built from AWS's official `public.ecr.aws/lambda/python` base image) also eliminated a separate cross-platform issue: packages `pip install`-ed on a local Mac/Windows machine produced binaries incompatible with Lambda's Amazon Linux runtime (`Runtime.ImportModuleError: Unable to import required dependency numpy`), which the container's Linux-native build environment resolves automatically.
- **Identical `encode_features` logic in training and inference** — both Lambdas run the exact same one-hot encoding function; the inference Lambda additionally calls `reindex(columns=feature_columns, fill_value=0)` against the column list saved in the model bundle at training time, guaranteeing a single-row inference request is encoded into precisely the same feature space the model was fit on, without requiring the client to pre-encode categorical values.
- **Loosely coupled training/inference** — the two Lambdas share no direct dependency or invocation path; they communicate exclusively through the Model S3 bucket (`model/bundle.pkl`, containing both the fitted model and `feature_columns`).

## AWS Well-Architected Framework Alignment

| Pillar | Implementation |
|---|---|
| **Operational Excellence** | Full IaC via CDK (zero manually created resources); structured `print`-based CloudWatch logging in all Lambda/Glue executions (row counts, RMSE/R², coefficients, encoding stages); CloudWatch Alarms on Lambda error metrics (`metric_errors()`) for all three Lambdas plus Glue job failed-task count; runbook included (see `runbook.md`) |
| **Security** | All 4 S3 buckets encrypted at rest (`S3_MANAGED`) with `BLOCK_ALL` public access; least-privilege IAM scoped per-service (Glue role: read Bronze/write Silver/read Scripts only; inference Lambda: read-only on Model bucket, cannot write; training Lambda: read Silver/write Model only); no hardcoded credentials anywhere — all AWS access via IAM roles; development conducted under a dedicated non-root IAM user rather than AWS account root credentials; AWS Academy Learner Lab IAM restrictions (blocked SageMaker execution role creation) documented as a lab constraint with the production equivalent explained |
| **Reliability** | S3 is inherently multi-AZ; Glue job configured with `max_retries=1` and a 10-minute timeout; Bronze data retained indefinitely (`RemovalPolicy.RETAIN`) enabling full reprocessing from raw source at any time; RTO ≈ 15 min (re-trigger pipeline from Bronze), RPO ≈ 0 (raw data never deleted) |
| **Performance Efficiency** | Glue Python Shell job capped at 1 DPU (vs. Spark's 2–100 DPU default) given the dataset's small flat-file scale; model bundle loaded at Lambda module scope (outside the handler) so it persists across warm invocations instead of re-fetching from S3 on every request;|

## Known Issues / Troubleshooting

- **Stale API Gateway responses after redeploying the inference Lambda.** During development, POST requests through the deployed `/predict` endpoint intermittently returned an identical cached-looking prediction regardless of the payload sent, even with API Gateway stage caching confirmed disabled. Direct Lambda invocation (bypassing API Gateway) correctly returned different predictions for different inputs, isolating the issue to the API Gateway stage rather than the Lambda or model logic. Resolved by forcing an explicit new stage deployment (Console: **API Gateway → Resources → Actions → Deploy API → stage: prod**) rather than relying on `cdk deploy` alone to refresh the integration. If predictions appear unexpectedly static after a Lambda code change, redeploy the API stage manually and retest.
- **`Runtime.ImportModuleError: numpy` on first Lambda Layer attempt.** Packages installed via plain `pip install` on a local Mac/Windows machine are compiled for that OS, not Lambda's Amazon Linux runtime. Resolved by moving to Docker container image deployment (see [Design Decisions](#design-decisions)).
- **Unrealistic predictions from hand-typed test payloads.** Sending attribute values far outside the training data's real distribution (e.g. `retail_shop_num` far below the observed minimum) produces an unreliable, extrapolated prediction — expected behavior for a linear model, not a pipeline bug. Use `scripts/validate_predictions.py` to test against real sampled data instead of hand-typed values.

---
