#!/usr/bin/env python3
"""
Changeset Capture & Notification System — Local Mock Simulation
===============================================================
Runs the full AWS cross-account pipeline locally. No AWS account needed.

Consumer registration is driven entirely by consumers.json —
add a new entry there and rerun; no code changes required.

Mocked services:
  S3            → moto  (in-memory, with cross-account bucket policy)
  SNS           → moto  (pub/sub with filter policies)
  SQS           → moto  (per-consumer queues in separate accounts + DLQs)
  SFTP server   → local filesystem  (mock/data/ directory)
  EMR Spark     → pandas DataFrames
  Step Functions → Python class orchestrator
  EventBridge   → direct Python trigger

Usage:
  pip install -r requirements.txt
  python simulation.py
"""

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from moto import mock_aws

# ── Fake AWS credentials (required by moto) ──────────────────────────────────
os.environ.setdefault("AWS_ACCESS_KEY_ID",     "mock-key-id")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
os.environ.setdefault("AWS_DEFAULT_REGION",    "us-east-1")

# ── Constants ─────────────────────────────────────────────────────────────────
REGION            = "us-east-1"
PUBLISHER_ACCOUNT = "123456789012"
BUCKET            = "mock-changeset-bucket"
TOPIC_NAME        = "entity-changesets"
DATA_DIR          = Path(__file__).parent / "data"
CONSUMERS_FILE    = Path(__file__).parent / "consumers.json"
DATE              = "2024-03-15"

# ── ANSI colours ──────────────────────────────────────────────────────────────
RESET   = "\033[0m";  BOLD    = "\033[1m";  DIM     = "\033[2m"
GREEN   = "\033[92m"; BLUE    = "\033[94m"; CYAN    = "\033[96m"
YELLOW  = "\033[93m"; RED     = "\033[91m"; MAGENTA = "\033[95m"


def ok(msg):    return f"{GREEN}  ✓ {RESET}{msg}"
def arrow(msg): return f"{CYAN}  → {RESET}{msg}"
def err(msg):   return f"{RED}  ✗ {RESET}{msg}"
def dim(msg):   return f"  {DIM}{msg}{RESET}"


def section(title, subtitle=""):
    width = 68
    sub   = f"  {DIM}{subtitle}{RESET}" if subtitle else ""
    print(f"\n{BOLD}{'═' * width}\n  {title}{sub}\n{'═' * width}{RESET}")


def step_header(name):
    print(f"\n  {BOLD}{BLUE}[STEP: {name}]{RESET}")


def load_registry() -> dict:
    with open(CONSUMERS_FILE) as f:
        return json.load(f)


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH DEFINITIONS  (mock input files — one INSERT, one UPDATE, one DELETE)
# ═══════════════════════════════════════════════════════════════════════════════

BATCHES = [
    {
        "batchId":    "batch-20240315-001",
        "changeType": "INSERT",
        "entityType": "Customer",
        "date":       DATE,
        "data_lines": [
            {"entityId":"ENT-101","firstName":"Alice","lastName":"Brown",
             "email":"alice@newco.com","phone":"+15550010101",
             "status":"ACTIVE","tier":"BRONZE","address":"10 New Lane",
             "country":"US","createdAt":"2024-03-15T08:00:00Z"},
            {"entityId":"ENT-102","firstName":"Bob","lastName":"Martinez",
             "email":"bob@techstart.io","phone":"+15550010202",
             "status":"ACTIVE","tier":"SILVER","address":"20 Startup Blvd",
             "country":"US","createdAt":"2024-03-15T08:01:00Z"},
            {"entityId":"ENT-103","firstName":"Carol","lastName":"White",
             "email":"carol@venture.com","phone":"+15550010303",
             "status":"ACTIVE","tier":"GOLD","address":"30 VC Drive",
             "country":"CA","createdAt":"2024-03-15T08:02:00Z"},
        ],
        "audit_lines": [
            {"entityId":"ENT-101","firstName":1,"lastName":1,"email":1,
             "phone":1,"status":1,"tier":1,"address":1,"country":1,"createdAt":1},
            {"entityId":"ENT-102","firstName":1,"lastName":1,"email":1,
             "phone":1,"status":1,"tier":1,"address":1,"country":1,"createdAt":1},
            {"entityId":"ENT-103","firstName":1,"lastName":1,"email":1,
             "phone":1,"status":1,"tier":1,"address":1,"country":1,"createdAt":1},
        ],
    },
    {
        "batchId":    "batch-20240315-002",
        "changeType": "UPDATE",
        "entityType": "Customer",
        "date":       DATE,
        "data_lines": [
            {"entityId":"ENT-001","email":"john.smith@acme.com","tier":"PLATINUM"},
            {"entityId":"ENT-002","phone":"+15550099999","status":"INACTIVE"},
            {"entityId":"ENT-003","address":"100 New Street","country":"US"},
        ],
        "audit_lines": [
            {"entityId":"ENT-001","firstName":0,"lastName":0,"email":1,
             "phone":0,"status":0,"tier":1,"address":0,"country":0,"createdAt":0},
            {"entityId":"ENT-002","firstName":0,"lastName":0,"email":0,
             "phone":1,"status":1,"tier":0,"address":0,"country":0,"createdAt":0},
            {"entityId":"ENT-003","firstName":0,"lastName":0,"email":0,
             "phone":0,"status":0,"tier":0,"address":1,"country":1,"createdAt":0},
        ],
    },
    {
        "batchId":    "batch-20240315-003",
        "changeType": "DELETE",
        "entityType": "Customer",
        "date":       DATE,
        "data_lines": [
            {"entityId":"ENT-004"},
            {"entityId":"ENT-005"},
        ],
        "audit_lines": [
            {"entityId":"ENT-004","firstName":1,"lastName":1,"email":1,
             "phone":1,"status":1,"tier":1,"address":1,"country":1,"createdAt":1},
            {"entityId":"ENT-005","firstName":1,"lastName":1,"email":1,
             "phone":1,"status":1,"tier":1,"address":1,"country":1,"createdAt":1},
        ],
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# AWS INFRASTRUCTURE
# ═══════════════════════════════════════════════════════════════════════════════

class AWSInfrastructure:
    """
    Sets up publisher-side resources (S3, SNS) and dynamically registers every
    consumer from consumers.json — each in their own AWS account.

    Cross-account pattern:
      1. Consumer creates SQS queue in their account
      2. Consumer adds SQS resource policy: allows SNS from publisher account
      3. Consumer registers queue ARN with publisher (entry in consumers.json)
      4. Publisher subscribes queue to SNS with consumer's filter policy
      5. Publisher grants consumer account s3:GetObject on changesets/* (if needed)
    """

    def __init__(self):
        self.s3        = boto3.client("s3",  region_name=REGION)
        self.sns       = boto3.client("sns", region_name=REGION)
        self.sqs       = boto3.client("sqs", region_name=REGION)
        self.topic_arn = None
        self.queues    = {}   # consumer name → {url, arn, cfg}
        self.registry  = load_registry()

    # ── Publisher resources ───────────────────────────────────────────────────

    def setup(self):
        section("INFRASTRUCTURE SETUP",
                f"publisher account: {PUBLISHER_ACCOUNT}  ·  region: {REGION}")

        self._create_s3()
        self._create_sns()
        self._apply_s3_cross_account_policy()
        self._register_all_consumers()
        self._print_consumer_table()

    def _create_s3(self):
        self.s3.create_bucket(Bucket=BUCKET)
        print(ok(f"S3 bucket:   {BUCKET}"))

    def _create_sns(self):
        resp           = self.sns.create_topic(Name=TOPIC_NAME)
        self.topic_arn = resp["TopicArn"]
        print(ok(f"SNS topic:   {TOPIC_NAME}"))
        print(dim(f"ARN: {self.topic_arn}"))

    def _apply_s3_cross_account_policy(self):
        """
        Grant s3:GetObject on changesets/* to every consumer account
        that declared s3ReadAccess: true.
        This lets consumer Lambdas fetch deltaRef / fullAuditRef payloads.
        """
        readers = [
            f"arn:aws:iam::{c['accountId']}:root"
            for c in self.registry["consumers"]
            if c.get("s3ReadAccess", False)
        ]
        if not readers:
            return

        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid":       "CrossAccountChangesetRead",
                    "Effect":    "Allow",
                    "Principal": {"AWS": readers},
                    "Action":    "s3:GetObject",
                    "Resource":  f"arn:aws:s3:::{BUCKET}/changesets/*",
                }
            ],
        }
        self.s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(policy))

        print(ok(f"S3 bucket policy: s3:GetObject on changesets/* granted to "
                 f"{len(readers)} consumer account(s)"))
        for r in readers:
            print(dim(f"  {r}"))

    # ── Consumer registration (generic — driven by consumers.json) ────────────

    def _register_all_consumers(self):
        consumers = self.registry["consumers"]
        print(f"\n  {BOLD}Registering {len(consumers)} consumers from consumers.json...{RESET}\n")

        for cfg in consumers:
            self._register_consumer(cfg)

    def _register_consumer(self, cfg: dict):
        """
        Simulate cross-account consumer onboarding:
          1. Consumer creates their SQS queue (in their account)
          2. Consumer applies SQS resource policy (allow SNS from publisher)
          3. Publisher creates SNS subscription with filter policy
        """
        name      = cfg["name"]
        acct      = cfg["accountId"]
        q_name    = cfg["queueName"]
        filter_p  = cfg["filterPolicy"]

        # ── Step 1: Consumer creates SQS queue + DLQ ─────────────────────
        dlq_url  = self.sqs.create_queue(QueueName=f"{q_name}-dlq")["QueueUrl"]
        dlq_arn  = self._queue_arn(dlq_url)

        q_url = self.sqs.create_queue(
            QueueName=q_name,
            Attributes={
                "RedrivePolicy": json.dumps({
                    "deadLetterTargetArn": dlq_arn,
                    "maxReceiveCount":     "3",
                }),
            },
        )["QueueUrl"]
        q_arn = self._queue_arn(q_url)

        # Rewrite ARN to show correct consumer account (moto uses publisher account)
        display_arn = f"arn:aws:sqs:{REGION}:{acct}:{q_name}"

        # ── Step 2: Cross-account SQS resource policy ─────────────────────
        cross_account_policy = {
            "Version": "2012-10-17",
            "Statement": [{
                "Sid":       "AllowSNSPublish",
                "Effect":    "Allow",
                "Principal": {"Service": "sns.amazonaws.com"},
                "Action":    "sqs:SendMessage",
                "Resource":  display_arn,
                "Condition": {
                    "ArnEquals": {"aws:SourceArn": self.topic_arn}
                },
            }],
        }
        # Apply policy (moto enforces it for subscription delivery)
        self.sqs.set_queue_attributes(
            QueueUrl=q_url,
            Attributes={"Policy": json.dumps({
                **cross_account_policy,
                "Statement": [{
                    **cross_account_policy["Statement"][0],
                    "Resource": q_arn,   # moto needs real ARN
                }],
            })},
        )

        # ── Step 3: SNS subscription with filter policy ───────────────────
        self.sns.subscribe(
            TopicArn=self.topic_arn,
            Protocol="sqs",
            Endpoint=q_arn,
            Attributes={"FilterPolicy": json.dumps(filter_p)},
        )

        self.queues[name] = {
            "url":         q_url,
            "arn":         q_arn,
            "display_arn": display_arn,
            "cfg":         cfg,
        }

        filter_str = "  +  ".join(
            f"{k}=[{', '.join(v)}]" for k, v in filter_p.items()
        )
        s3_icon = "✓ s3:GetObject" if cfg.get("s3ReadAccess") else "✗ no S3 access"

        print(f"  {GREEN}✓{RESET} {BOLD}[{acct}]{RESET} {name:<22} "
              f"{DIM}({cfg['team']}){RESET}")
        print(dim(f"    Queue:  {display_arn}"))
        print(dim(f"    Filter: {filter_str}"))
        print(dim(f"    S3:     {s3_icon}"))
        print(dim(f"    SQS resource policy: sns.amazonaws.com → ALLOW sqs:SendMessage"))
        print()

    def _print_consumer_table(self):
        """Print a compact summary table of all registered consumers."""
        consumers = self.registry["consumers"]
        col = [22, 14, 35, 28]
        hdr = ["Consumer", "Account", "Filter Policy", "Description"]

        print(f"  {BOLD}", end="")
        for h, w in zip(hdr, col):
            print(f"{h:<{w}}", end="")
        print(RESET)
        print("  " + "─" * 100)

        for c in consumers:
            fp = "  +  ".join(
                f"{k}=[{','.join(v)}]" for k, v in c["filterPolicy"].items()
            )
            row = [c["name"], c["accountId"], fp, c["description"]]
            print("  " + "".join(f"{str(v):<{w}}" for v, w in zip(row, col)))

    def _queue_arn(self, url: str) -> str:
        return self.sqs.get_queue_attributes(
            QueueUrl=url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]


# ═══════════════════════════════════════════════════════════════════════════════
# SFTP MOCK INGESTOR
# ═══════════════════════════════════════════════════════════════════════════════

class SFTPIngestor:

    def __init__(self, infra: AWSInfrastructure):
        self.infra = infra

    def ingest(self, batch: dict) -> tuple[str, str]:
        step_header("SFTP INGESTOR")
        print(arrow("Connecting to mock SFTP server (sftp.internal.example.com:22)..."))
        print(arrow("Authenticated via SSH key"))

        bid   = batch["batchId"]
        ctype = batch["changeType"]
        date  = batch["date"]

        data_content  = "\n".join(json.dumps(r) for r in batch["data_lines"])
        audit_content = "\n".join(json.dumps(r) for r in batch["audit_lines"])

        data_fname  = f"data_{ctype}_{bid}_{date.replace('-','')}.txt"
        audit_fname = f"audit_{ctype}_{bid}_{date.replace('-','')}.txt"
        prefix      = f"landing/{date}/{bid}"
        data_key    = f"{prefix}/{data_fname}"
        audit_key   = f"{prefix}/{audit_fname}"

        self.infra.s3.put_object(Bucket=BUCKET, Key=data_key,  Body=data_content.encode())
        self.infra.s3.put_object(Bucket=BUCKET, Key=audit_key, Body=audit_content.encode())

        print(ok(f"Uploaded: s3://{BUCKET}/{data_key}  ({len(data_content):,} B)"))
        print(ok(f"Uploaded: s3://{BUCKET}/{audit_key}  ({len(audit_content):,} B)"))
        print(arrow("EventBridge S3 rule detected audit file → triggering Step Functions"))

        return data_key, audit_key


# ═══════════════════════════════════════════════════════════════════════════════
# EMR PROCESSOR  (pandas replaces Spark)
# ═══════════════════════════════════════════════════════════════════════════════

class EMRProcessor:

    def __init__(self, infra: AWSInfrastructure):
        self.infra = infra

    def process(self, batch: dict, data_key: str, audit_key: str) -> dict:
        step_header("EMR SERVERLESS  (pandas mock of Apache Spark)")
        bid    = batch["batchId"]
        ctype  = batch["changeType"]
        date   = batch["date"]
        etype  = batch["entityType"]
        job_id = f"j-MOCK{bid[-3:].upper()}"

        print(arrow(f"Job ID:  {job_id}"))
        print(arrow("Config:  1 driver (4 vCPU / 16 GB)  +  2 executors (4 vCPU / 16 GB each)"))

        # Read from S3
        data_lines  = self._read_s3_lines(data_key)
        audit_lines = self._read_s3_lines(audit_key)
        data_df     = pd.DataFrame([json.loads(l) for l in data_lines])
        audit_df    = pd.DataFrame([json.loads(l) for l in audit_lines])

        print(ok(f"Read {len(data_df)} records  ← data file"))
        print(ok(f"Read {len(audit_df)} records  ← audit file"))

        # Join on entityId
        merged = data_df.merge(audit_df, on="entityId", suffixes=("_data", "_audit"))
        print(ok(f"Inner join on entityId → {len(merged)} matched rows"))

        # Extract changesets
        audit_cols = [c for c in audit_df.columns if c != "entityId"]
        changesets = []
        for _, row in merged.iterrows():
            changed = [c for c in audit_cols
                       if row.get(f"{c}_audit", row.get(c, 0)) == 1]
            delta   = {}
            for f in changed:
                val = row.get(f"{f}_data", row.get(f))
                if val is not None and not (isinstance(val, float) and pd.isna(val)):
                    delta[f] = val
            changesets.append({
                "entityId":      row["entityId"],
                "changeType":    ctype,
                "changedFields": changed,
                "deltaPayload":  delta,
            })

        self._print_changeset_table(changesets)

        # Write to S3 processed zone
        ct_dir      = ctype.lower() + "s"
        base_prefix = f"changesets/{etype}/{date}/{bid}"
        for cs in changesets:
            eid = cs["entityId"]
            self.infra.s3.put_object(
                Bucket=BUCKET,
                Key=f"{base_prefix}/{ct_dir}/{eid}_delta.json",
                Body=json.dumps(cs["deltaPayload"]).encode(),
            )
            if ctype in ("UPDATE", "DELETE"):
                audit_row = audit_df[audit_df["entityId"] == eid].to_dict("records")[0]
                self.infra.s3.put_object(
                    Bucket=BUCKET,
                    Key=f"{base_prefix}/{ct_dir}/{eid}_audit.json",
                    Body=json.dumps(audit_row).encode(),
                )

        # Write manifest
        manifest = {
            "batchId":        bid,
            "entityType":     etype,
            "changeType":     ctype,
            "processedAt":    datetime.now(timezone.utc).isoformat(),
            "sourceDataFile":  f"s3://{BUCKET}/{data_key}",
            "sourceAuditFile": f"s3://{BUCKET}/{audit_key}",
            "stats": {
                "totalRecords": len(changesets),
                "inserts": len(changesets) if ctype == "INSERT" else 0,
                "updates": len(changesets) if ctype == "UPDATE" else 0,
                "deletes": len(changesets) if ctype == "DELETE" else 0,
            },
            "paths":      {ct_dir: f"s3://{BUCKET}/{base_prefix}/{ct_dir}/"},
            "emrJobId":   job_id,
            "batchDate":  date,
            "changesets": changesets,
        }
        manifest_key = f"{base_prefix}/manifest.json"
        self.infra.s3.put_object(
            Bucket=BUCKET, Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode(),
        )
        print(ok(f"Written {len(changesets)} files → s3://{BUCKET}/{base_prefix}/{ct_dir}/"))
        print(ok(f"Written manifest      → s3://{BUCKET}/{manifest_key}"))
        return manifest

    def _read_s3_lines(self, key: str) -> list[str]:
        return self.infra.s3.get_object(
            Bucket=BUCKET, Key=key
        )["Body"].read().decode().strip().splitlines()

    def _print_changeset_table(self, changesets: list[dict]):
        col_w  = [12, 10, 35, 0]
        header = ["entityId", "changeType", "changedFields", "deltaPayload (preview)"]
        print(f"\n    {BOLD}", end="")
        for h, w in zip(header, col_w):
            print(f"{h:<{w}}" if w else h, end="  ")
        print(RESET)
        print("    " + "─" * 85)
        for cs in changesets:
            items   = list(cs["deltaPayload"].items())[:3]
            preview = ", ".join(f"{k}={json.dumps(v)}" for k, v in items)
            if len(cs["deltaPayload"]) > 3:
                preview += " ..."
            print(f"    {cs['entityId']:<12}  {cs['changeType']:<10}  "
                  f"{', '.join(cs['changedFields']):<35}  {preview}")
        print()


# ═══════════════════════════════════════════════════════════════════════════════
# SNS PUBLISHER
# ═══════════════════════════════════════════════════════════════════════════════

class SNSPublisher:

    def __init__(self, infra: AWSInfrastructure):
        self.infra = infra

    def publish(self, manifest: dict):
        step_header("PUBLISH CHANGESETS → SNS")
        bid    = manifest["batchId"]
        ctype  = manifest["changeType"]
        etype  = manifest["entityType"]
        date   = manifest["batchDate"]
        ct_dir = ctype.lower() + "s"
        n      = manifest["stats"]["totalRecords"]

        print(arrow(f"Publishing {n} entity events  +  1 batch manifest"))
        print(arrow("SNS filter policies route to subscribed consumer accounts\n"))

        # Per-entity ENTITY_CHANGESET events
        for cs in manifest["changesets"]:
            eid   = cs["entityId"]
            event = {
                "eventId":      str(uuid.uuid4()),
                "eventVersion": "1.0",
                "eventType":    "ENTITY_CHANGESET",
                "changeType":   ctype,
                "entityType":   etype,
                "entityId":     eid,
                "batchId":      bid,
                "occurredAt":   datetime.now(timezone.utc).isoformat(),
                "processedAt":  manifest["processedAt"],
                "changeset": {
                    "changedFields": cs["changedFields"],
                    "deltaRef":
                        f"s3://{BUCKET}/changesets/{etype}/{date}/{bid}/{ct_dir}/{eid}_delta.json",
                    "fullAuditRef":
                        f"s3://{BUCKET}/changesets/{etype}/{date}/{bid}/{ct_dir}/{eid}_audit.json",
                },
                "source": {
                    "dataFile":  manifest["sourceDataFile"],
                    "auditFile": manifest["sourceAuditFile"],
                },
                "processing": {"emrJobId": manifest["emrJobId"]},
            }
            resp = self.infra.sns.publish(
                TopicArn=self.infra.topic_arn,
                Message=json.dumps(event),
                MessageAttributes={
                    "eventType":    {"DataType": "String", "StringValue": "ENTITY_CHANGESET"},
                    "changeType":   {"DataType": "String", "StringValue": ctype},
                    "entityType":   {"DataType": "String", "StringValue": etype},
                    "batchId":      {"DataType": "String", "StringValue": bid},
                    "eventVersion": {"DataType": "String", "StringValue": "1.0"},
                },
            )
            print(ok(f"[ENTITY_CHANGESET / {ctype:6s}]  {eid}  "
                     f"MessageId={resp['MessageId'][:8]}..."))

        # BATCH_MANIFEST event
        batch_event = {
            "eventId":      str(uuid.uuid4()),
            "eventVersion": "1.0",
            "eventType":    "BATCH_MANIFEST",
            "batchId":      bid,
            "changeType":   ctype,
            "entityType":   etype,
            "processedAt":  manifest["processedAt"],
            "stats":        manifest["stats"],
            "paths":        manifest["paths"],
        }
        resp = self.infra.sns.publish(
            TopicArn=self.infra.topic_arn,
            Message=json.dumps(batch_event),
            MessageAttributes={
                "eventType":  {"DataType": "String", "StringValue": "BATCH_MANIFEST"},
                "entityType": {"DataType": "String", "StringValue": etype},
                "batchId":    {"DataType": "String", "StringValue": bid},
            },
        )
        print(ok(f"[BATCH_MANIFEST      ]  batchId={bid}  "
                 f"MessageId={resp['MessageId'][:8]}..."))


# ═══════════════════════════════════════════════════════════════════════════════
# STEP FUNCTIONS ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

class StepFunctionsWorkflow:

    def __init__(self, infra: AWSInfrastructure):
        self.infra = infra
        self.emr   = EMRProcessor(infra)
        self.pub   = SNSPublisher(infra)

    def execute(self, batch: dict, data_key: str, audit_key: str) -> dict:
        exec_id = (f"arn:aws:states:{REGION}:{PUBLISHER_ACCOUNT}:"
                   f"execution:changeset-sfn:{uuid.uuid4()}")
        print(arrow(f"ExecutionArn: {exec_id[:72]}..."))

        self._validate_pair(data_key, audit_key)
        job_id = self._create_emr(batch["batchId"])
        self._mapping_step(batch)
        manifest = self.emr.process(batch, data_key, audit_key)
        self._terminate_emr(job_id)
        self.pub.publish(manifest)
        return manifest

    def _validate_pair(self, data_key, audit_key):
        step_header("ValidatePair  ← NEW STATE")
        for key, label in [(data_key, "data file"), (audit_key, "audit file")]:
            try:
                self.infra.s3.head_object(Bucket=BUCKET, Key=key)
                print(ok(f"{label} present: s3://{BUCKET}/{key}"))
            except Exception:
                print(err(f"{label} MISSING — alarm firing, execution FAILED"))
                raise RuntimeError(f"Pair validation failed: {key}")
        print(ok("naming convention valid"))
        print(ok("ValidatePair → PASSED"))

    def _create_emr(self, batch_id):
        step_header("CreateEMRCluster  ← REUSED")
        job_id = f"j-MOCK{batch_id[-3:].upper()}"
        print(ok(f"EMR Serverless application ID: {job_id}"))
        return job_id

    def _mapping_step(self, batch):
        step_header("MappingStep  ← REUSED")
        print(ok("tier normalised:    BRONZE→1  SILVER→2  GOLD→3  PLATINUM→4"))
        print(ok("country normalised: CA→CAN  US→USA  UK→GBR"))

    def _terminate_emr(self, job_id):
        step_header("TerminateEMR  ← REUSED")
        print(ok(f"Job {job_id} completed — cluster terminated"))


# ═══════════════════════════════════════════════════════════════════════════════
# GENERIC CONSUMER
# Each consumer lives in its own AWS account.
# Processing behaviour is determined by processingType from consumers.json.
# ═══════════════════════════════════════════════════════════════════════════════

class Consumer:
    """Generic consumer — processes SQS messages from its cross-account queue."""

    def __init__(self, cfg: dict, infra: AWSInfrastructure):
        self.cfg   = cfg
        self.infra = infra
        self.name  = cfg["name"]
        self.acct  = cfg["accountId"]
        self.msgs  = []

    def drain(self) -> list[dict]:
        q_url    = self.infra.queues[self.name]["url"]
        messages = []
        while True:
            resp  = self.infra.sqs.receive_message(
                QueueUrl=q_url, MaxNumberOfMessages=10, WaitTimeSeconds=0
            )
            batch = resp.get("Messages", [])
            if not batch:
                break
            for m in batch:
                body  = json.loads(m["Body"])
                event = json.loads(body.get("Message", m["Body"]))
                messages.append(event)
                self.infra.sqs.delete_message(
                    QueueUrl=q_url, ReceiptHandle=m["ReceiptHandle"]
                )
        self.msgs = messages
        return messages

    def process(self):
        msgs  = self.drain()
        ptype = self.cfg.get("processingType", "generic")
        label = self.cfg["description"]

        print(f"\n  {BOLD}{MAGENTA}[Account {self.acct}]  {self.name}{RESET}  "
              f"— {len(msgs)} messages")
        print(dim(f"    Team: {self.cfg['team']}  ·  {label}"))

        if not msgs:
            print(dim("    (no messages received — filtered by SNS policy)"))
            return

        # Route to the appropriate display logic by processingType
        dispatch = {
            "microservice": self._process_microservice,
            "search":       self._process_search,
            "audit":        self._process_audit,
            "analytics":    self._process_analytics,
            "datalake":     self._process_datalake,
        }
        dispatch.get(ptype, self._process_generic)(msgs)

    def _process_microservice(self, msgs):
        for m in msgs:
            et = m.get("eventType", "?")
            if et == "ENTITY_CHANGESET":
                ctype  = m.get("changeType", "?")
                eid    = m.get("entityId", "?")
                fields = ", ".join(m.get("changeset", {}).get("changedFields", []))
                ref    = m.get("changeset", {}).get("deltaRef", "")
                print(f"    [{ctype:6s}] {eid}  changed: [{fields}]")
                print(dim(f"           → GET {ref}"))

    def _process_search(self, msgs):
        for m in msgs:
            ctype  = m.get("changeType", "?")
            eid    = m.get("entityId", "?")
            fields = ", ".join(m.get("changeset", {}).get("changedFields", []))
            print(f"    [{ctype:6s}] {eid}  re-indexing: [{fields}]")

    def _process_audit(self, msgs):
        for m in msgs:
            ctype = m.get("changeType", "?")
            eid   = m.get("entityId", "?")
            eid_s = m.get("eventId", "?")[:8]
            ts    = m.get("processedAt", "?")[:19]
            print(f"    [{ctype:6s}] {eid}  eventId={eid_s}...  ts={ts}  → audit store")

    def _process_analytics(self, msgs):
        col_w = [32, 12, 8, 9, 9, 9]
        hdr   = ["batchId", "changeType", "total", "inserts", "updates", "deletes"]
        print(f"    {BOLD}", end="")
        for h, w in zip(hdr, col_w):
            print(f"{h:<{w}}", end="")
        print(RESET)
        print("    " + "─" * 80)
        for m in msgs:
            if m.get("eventType") == "BATCH_MANIFEST":
                s   = m.get("stats", {})
                row = [m.get("batchId","?"), m.get("changeType","?"),
                       s.get("totalRecords",0), s.get("inserts",0),
                       s.get("updates",0), s.get("deletes",0)]
                print("    " + "".join(f"{str(v):<{w}}" for v, w in zip(row, col_w)))
        print(dim("    → COPY command issued to Redshift"))

    def _process_datalake(self, msgs):
        entity_msgs   = [m for m in msgs if m.get("eventType") == "ENTITY_CHANGESET"]
        manifest_msgs = [m for m in msgs if m.get("eventType") == "BATCH_MANIFEST"]
        print(f"    {len(entity_msgs)} ENTITY_CHANGESET events  +  "
              f"{len(manifest_msgs)} BATCH_MANIFEST events")
        for m in entity_msgs:
            ctype = m.get("changeType","?")
            eid   = m.get("entityId","?")
            print(dim(f"    [{ctype:6s}] {eid}  → written to Data Lake partition"))
        for m in manifest_msgs:
            print(dim(f"    [MANIFEST] {m.get('batchId','?')}  stats={m.get('stats',{})}"))

    def _process_generic(self, msgs):
        for m in msgs:
            print(dim(f"    eventType={m.get('eventType','?')}  "
                      f"entityId={m.get('entityId','?')}"))


# ═══════════════════════════════════════════════════════════════════════════════
# CONSUMER ONBOARDING GUIDE
# ═══════════════════════════════════════════════════════════════════════════════

def print_onboarding_guide(registry: dict):
    section("HOW TO ONBOARD A NEW CONSUMER")
    topic_arn = (f"arn:aws:sns:{registry['region']}:"
                 f"{registry['publisherAccountId']}:{registry['snsTopicName']}")

    print(f"""  Any team in any AWS account can subscribe in 3 steps:

  {BOLD}Step 1 — Consumer creates their SQS queue (in their AWS account){RESET}

    aws sqs create-queue --queue-name <your-queue-name> \\
        --attributes RedrivePolicy='{{"deadLetterTargetArn":"<dlq-arn>","maxReceiveCount":"3"}}'

  {BOLD}Step 2 — Consumer applies cross-account SQS resource policy{RESET}

    {{
      "Version": "2012-10-17",
      "Statement": [{{
        "Effect":    "Allow",
        "Principal": {{"Service": "sns.amazonaws.com"}},
        "Action":    "sqs:SendMessage",
        "Resource":  "arn:aws:sqs:{registry['region']}:<YOUR_ACCOUNT>:<your-queue>",
        "Condition": {{"ArnEquals": {{"aws:SourceArn": "{topic_arn}"}}}}
      }}]
    }}

  {BOLD}Step 3 — Add an entry to consumers.json and notify the publisher team{RESET}

    {{
      "name":          "<your-team-name>",
      "accountId":     "<YOUR_AWS_ACCOUNT_ID>",
      "region":        "{registry['region']}",
      "queueName":     "<your-queue-name>",
      "team":          "<Team Name>",
      "contact":       "<email>",
      "description":   "<what you do with the events>",
      "filterPolicy":  {{"eventType": ["ENTITY_CHANGESET"]}},
      "s3ReadAccess":  true
    }}

  {DIM}Available filterPolicy values:
    eventType:  ENTITY_CHANGESET  (per-entity events)
                BATCH_MANIFEST    (one summary per batch — for analytics / bulk loads)
    changeType: INSERT, UPDATE, DELETE  (only applies to ENTITY_CHANGESET){RESET}
""")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

@mock_aws
def run_simulation():
    registry = load_registry()
    n_consumers = len(registry["consumers"])

    print(f"\n{BOLD}{'═' * 68}")
    print(f"  CHANGESET CAPTURE & NOTIFICATION SYSTEM")
    print(f"  Cross-Account Mock  ·  date={DATE}  ·  "
          f"{len(BATCHES)} batches  ·  {n_consumers} consumers")
    print(f"{'═' * 68}{RESET}")

    # 1. Infrastructure
    infra = AWSInfrastructure()
    infra.setup()

    sfn           = StepFunctionsWorkflow(infra)
    sftp          = SFTPIngestor(infra)
    all_manifests = []

    # 2. Process each batch
    for i, batch in enumerate(BATCHES, 1):
        n = len(batch["data_lines"])
        section(f"BATCH {i}/{len(BATCHES)}  ▶  {batch['changeType']}",
                f"batchId={batch['batchId']}  ·  {n} entit{'y' if n == 1 else 'ies'}")

        data_key, audit_key = sftp.ingest(batch)
        print()
        step_header("Step Functions  StartExecution")
        manifest = sfn.execute(batch, data_key, audit_key)
        all_manifests.append(manifest)

    # 3. Each consumer (in their own account) drains their SQS queue
    section("CONSUMER PROCESSING",
            f"{n_consumers} consumers across {n_consumers} AWS accounts")

    consumers = [
        Consumer(cfg, infra)
        for cfg in registry["consumers"]
    ]
    for c in consumers:
        c.process()

    # 4. SNS routing summary
    section("SNS → SQS ROUTING SUMMARY")
    col_w = [22, 14, 8, 55]
    hdr   = ["Consumer", "Account", "Received", "SNS Filter Policy Applied"]
    print(f"  {BOLD}", end="")
    for h, w in zip(hdr, col_w):
        print(f"{h:<{w}}", end="")
    print(RESET)
    print("  " + "─" * 100)

    for c in consumers:
        fp_str = "  +  ".join(
            f"{k}=[{','.join(v)}]"
            for k, v in c.cfg["filterPolicy"].items()
        )
        row = [c.name, c.acct, str(len(c.msgs)), fp_str]
        print("  " + "".join(f"{v:<{w}}" for v, w in zip(row, col_w)))

    # 5. S3 inventory
    section("S3 BUCKET CONTENTS", f"s3://{BUCKET}  (publisher account {PUBLISHER_ACCOUNT})")
    objs   = infra.s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    groups: dict[str, list] = {}
    for o in objs:
        parts  = o["Key"].split("/")
        prefix = f"{parts[0]}/{parts[1]}" if len(parts) > 1 else parts[0]
        groups.setdefault(prefix, []).append(o)

    for prefix, items in sorted(groups.items()):
        total = sum(o["Size"] for o in items)
        print(f"  {CYAN}{prefix}/{RESET}  ({len(items)} objects, {total:,} bytes)")
        for o in sorted(items, key=lambda x: x["Key"])[:3]:
            print(dim(f"    └─ {o['Key'].split('/')[-1]}  ({o['Size']} B)"))
        if len(items) > 3:
            print(dim(f"       … and {len(items) - 3} more"))

    # 6. Onboarding guide
    print_onboarding_guide(registry)

    # 7. Final summary
    total_entities = sum(m["stats"]["totalRecords"] for m in all_manifests)
    inserts  = sum(m["stats"]["inserts"] for m in all_manifests)
    updates  = sum(m["stats"]["updates"] for m in all_manifests)
    deletes  = sum(m["stats"]["deletes"] for m in all_manifests)
    total_ev = total_entities + len(BATCHES)

    section("SIMULATION COMPLETE ✓")
    print(f"  Publisher account:   {PUBLISHER_ACCOUNT}")
    print(f"  Consumer accounts:   {n_consumers}  "
          f"({', '.join(c['accountId'] for c in registry['consumers'])})")
    print(f"  Batches processed:   {len(BATCHES)}")
    print(f"  Entities:            {total_entities}  "
          f"(inserts={inserts}  updates={updates}  deletes={deletes})")
    print(f"  SNS events:          {total_ev}  "
          f"({total_entities} entity  +  {len(BATCHES)} batch manifests)")
    print(f"  S3 objects:          {len(objs)}")
    print(f"\n  {YELLOW}Estimated real AWS cost: ~$0.83/run{RESET}  {DIM}(moto is free!){RESET}")
    print(f"\n  {DIM}To add a consumer: edit mock/consumers.json and rerun.{RESET}\n")


if __name__ == "__main__":
    run_simulation()
