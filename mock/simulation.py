#!/usr/bin/env python3
"""
Changeset Capture & Notification System — Local Mock Simulation
===============================================================
Runs the full AWS pipeline locally. No AWS account needed.

Mocked services:
  S3            → moto  (in-memory object store)
  SNS           → moto  (pub/sub with filter policies)
  SQS           → moto  (per-consumer queues + DLQs)
  SFTP server   → local filesystem (mock/data/ directory)
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

# ── Fake AWS credentials required by moto ────────────────────────────────────
os.environ.setdefault("AWS_ACCESS_KEY_ID", "mock-key-id")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "mock-secret-key")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

# ── Constants ─────────────────────────────────────────────────────────────────
REGION     = "us-east-1"
BUCKET     = "mock-changeset-bucket"
TOPIC_NAME = "entity-changesets"
DATA_DIR   = Path(__file__).parent / "data"
DATE       = "2024-03-15"

# ── ANSI colour helpers ───────────────────────────────────────────────────────
RESET   = "\033[0m"
BOLD    = "\033[1m"
DIM     = "\033[2m"
GREEN   = "\033[92m"
BLUE    = "\033[94m"
CYAN    = "\033[96m"
YELLOW  = "\033[93m"
RED     = "\033[91m"
MAGENTA = "\033[95m"


def ok(msg):    return f"{GREEN}  ✓ {RESET}{msg}"
def arrow(msg): return f"{CYAN}  → {RESET}{msg}"
def err(msg):   return f"{RED}  ✗ {RESET}{msg}"


def section(title, subtitle=""):
    width = 68
    sep   = "═" * width
    sub   = f"  {DIM}{subtitle}{RESET}" if subtitle else ""
    print(f"\n{BOLD}{sep}")
    print(f"  {title}{sub}")
    print(f"{sep}{RESET}")


def step_header(name):
    print(f"\n  {BOLD}{BLUE}[STEP: {name}]{RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# BATCH DEFINITIONS
# Each batch = one file drop: INSERT, UPDATE, or DELETE
# ═══════════════════════════════════════════════════════════════════════════════

BATCHES = [
    {
        "batchId":    "batch-20240315-001",
        "changeType": "INSERT",
        "entityType": "Customer",
        "date":       DATE,
        # Data file: full records for new entities
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
        # Audit file: all 1s (every field is "new")
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
        # Data file: only changed fields per entity
        "data_lines": [
            {"entityId":"ENT-001","email":"john.smith@acme.com","tier":"PLATINUM"},
            {"entityId":"ENT-002","phone":"+15550099999","status":"INACTIVE"},
            {"entityId":"ENT-003","address":"100 New Street","country":"US"},
        ],
        # Audit file: full row; 1 = changed, 0 = unchanged
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
        # Data file: entity ID only
        "data_lines": [
            {"entityId":"ENT-004"},
            {"entityId":"ENT-005"},
        ],
        # Audit file: all 1s (full entity removed)
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
# Creates S3 bucket, SNS topic, SQS queues + subscriptions via moto
# ═══════════════════════════════════════════════════════════════════════════════

class AWSInfrastructure:
    """Sets up all mocked AWS resources."""

    CONSUMER_CONFIG = [
        {
            "name":   "microservice-consumer",
            "filter": {"eventType": ["ENTITY_CHANGESET"]},
            "label":  "all change types",
        },
        {
            "name":   "search-consumer",
            "filter": {"eventType": ["ENTITY_CHANGESET"], "changeType": ["INSERT", "UPDATE"]},
            "label":  "INSERT + UPDATE only",
        },
        {
            "name":   "audit-consumer",
            "filter": {"eventType": ["ENTITY_CHANGESET"]},
            "label":  "all change types",
        },
        {
            "name":   "analytics-consumer",
            "filter": {"eventType": ["BATCH_MANIFEST"]},
            "label":  "BATCH_MANIFEST only",
        },
    ]

    def __init__(self):
        self.s3        = boto3.client("s3",  region_name=REGION)
        self.sns       = boto3.client("sns", region_name=REGION)
        self.sqs       = boto3.client("sqs", region_name=REGION)
        self.topic_arn = None
        self.queues    = {}   # name → {url, arn, label}

    def setup(self):
        section("INFRASTRUCTURE SETUP", "creating mocked AWS resources")
        self._create_s3()
        self._create_sns()
        self._create_queues()

    # ── S3 ───────────────────────────────────────────────────────────────────

    def _create_s3(self):
        self.s3.create_bucket(Bucket=BUCKET)
        print(ok(f"S3 bucket:        {BUCKET}"))

    # ── SNS ──────────────────────────────────────────────────────────────────

    def _create_sns(self):
        resp           = self.sns.create_topic(Name=TOPIC_NAME)
        self.topic_arn = resp["TopicArn"]
        print(ok(f"SNS topic:        {TOPIC_NAME}"))
        print(ok(f"  ARN:            {self.topic_arn}"))

    # ── SQS queues + SNS subscriptions ───────────────────────────────────────

    def _create_queues(self):
        for cfg in self.CONSUMER_CONFIG:
            name = cfg["name"]

            # DLQ
            dlq_url  = self.sqs.create_queue(QueueName=f"{name}-dlq")["QueueUrl"]
            dlq_arn  = self._queue_arn(dlq_url)

            # Main queue
            q_url = self.sqs.create_queue(
                QueueName=name,
                Attributes={
                    "RedrivePolicy": json.dumps({
                        "deadLetterTargetArn": dlq_arn,
                        "maxReceiveCount":     "3",
                    })
                },
            )["QueueUrl"]
            q_arn = self._queue_arn(q_url)

            # Allow SNS to write to this queue
            self.sqs.set_queue_attributes(
                QueueUrl=q_url,
                Attributes={
                    "Policy": json.dumps({
                        "Version": "2012-10-17",
                        "Statement": [{
                            "Effect":    "Allow",
                            "Principal": {"Service": "sns.amazonaws.com"},
                            "Action":    "sqs:SendMessage",
                            "Resource":  q_arn,
                            "Condition": {"ArnEquals": {"aws:SourceArn": self.topic_arn}},
                        }],
                    })
                },
            )

            # Subscribe with filter policy
            self.sns.subscribe(
                TopicArn=self.topic_arn,
                Protocol="sqs",
                Endpoint=q_arn,
                Attributes={"FilterPolicy": json.dumps(cfg["filter"])},
            )

            self.queues[name] = {"url": q_url, "arn": q_arn, "label": cfg["label"]}
            print(ok(f"SQS queue:        {name:<38} [{cfg['label']}]"))

    def _queue_arn(self, url: str) -> str:
        return self.sqs.get_queue_attributes(
            QueueUrl=url, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]


# ═══════════════════════════════════════════════════════════════════════════════
# SFTP MOCK INGESTOR
# Simulates pulling files from an external SFTP server → uploads to S3 landing
# ═══════════════════════════════════════════════════════════════════════════════

class SFTPIngestor:

    def __init__(self, infra: AWSInfrastructure):
        self.infra = infra

    def ingest(self, batch: dict) -> tuple[str, str]:
        """Simulate SFTP pull: write batch files to S3 landing zone."""
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
# Reads data + audit from S3, joins on entityId, extracts changesets, writes output
# ═══════════════════════════════════════════════════════════════════════════════

class EMRProcessor:

    def __init__(self, infra: AWSInfrastructure):
        self.infra = infra

    def process(self, batch: dict, data_key: str, audit_key: str) -> dict:
        step_header("EMR SERVERLESS  (pandas mock of Apache Spark)")
        bid   = batch["batchId"]
        ctype = batch["changeType"]
        date  = batch["date"]
        etype = batch["entityType"]
        job_id = f"j-MOCK{bid[-3:].upper()}"

        print(arrow(f"Job ID:  {job_id}"))
        print(arrow(f"Config:  1 driver (4 vCPU / 16 GB)  +  2 executors (4 vCPU / 16 GB each)"))

        # ── Read from S3 ─────────────────────────────────────────────────────
        data_lines  = self._read_s3_lines(data_key)
        audit_lines = self._read_s3_lines(audit_key)

        data_df  = pd.DataFrame([json.loads(l) for l in data_lines])
        audit_df = pd.DataFrame([json.loads(l) for l in audit_lines])

        print(ok(f"Read {len(data_df)} records  ← data file"))
        print(ok(f"Read {len(audit_df)} records  ← audit file"))

        # ── Join on entityId ──────────────────────────────────────────────────
        merged = data_df.merge(audit_df, on="entityId", suffixes=("_data", "_audit"))
        print(ok(f"Inner join on entityId → {len(merged)} matched rows"))

        # ── Extract changesets ────────────────────────────────────────────────
        audit_cols = [c for c in audit_df.columns if c != "entityId"]
        changesets = []

        for _, row in merged.iterrows():
            # Fields where audit flag == 1
            changed = [c for c in audit_cols if row.get(f"{c}_audit", row.get(c, 0)) == 1]

            # Build delta payload from data file values
            delta = {}
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

        # ── Print changeset table ─────────────────────────────────────────────
        self._print_changeset_table(changesets)

        # ── Write individual entity files to S3 processed zone ───────────────
        ct_dir      = ctype.lower() + "s"   # inserts / updates / deletes
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

        # ── Write manifest ────────────────────────────────────────────────────
        manifest = {
            "batchId":       bid,
            "entityType":    etype,
            "changeType":    ctype,
            "processedAt":   datetime.now(timezone.utc).isoformat(),
            "sourceDataFile":  f"s3://{BUCKET}/{data_key}",
            "sourceAuditFile": f"s3://{BUCKET}/{audit_key}",
            "stats": {
                "totalRecords": len(changesets),
                "inserts": len(changesets) if ctype == "INSERT" else 0,
                "updates": len(changesets) if ctype == "UPDATE" else 0,
                "deletes": len(changesets) if ctype == "DELETE" else 0,
            },
            "paths": {ct_dir: f"s3://{BUCKET}/{base_prefix}/{ct_dir}/"},
            "emrJobId":   job_id,
            "batchDate":  date,
            "changesets": changesets,  # passed to publisher; not in real manifest
        }

        manifest_key = f"{base_prefix}/manifest.json"
        self.infra.s3.put_object(
            Bucket=BUCKET, Key=manifest_key,
            Body=json.dumps(manifest, indent=2).encode(),
        )

        print(ok(f"Written {len(changesets)} changeset files → s3://{BUCKET}/{base_prefix}/{ct_dir}/"))
        print(ok(f"Written manifest       → s3://{BUCKET}/{manifest_key}"))

        return manifest

    def _read_s3_lines(self, key: str) -> list[str]:
        obj = self.infra.s3.get_object(Bucket=BUCKET, Key=key)
        return obj["Body"].read().decode().strip().splitlines()

    def _print_changeset_table(self, changesets: list[dict]):
        col_w = [12, 10, 35, 0]
        header = ["entityId", "changeType", "changedFields", "deltaPayload (preview)"]
        sep    = "─" * 85

        print(f"\n    {BOLD}", end="")
        for h, w in zip(header, col_w):
            print(f"{h:<{w}}" if w else h, end="  ")
        print(RESET)
        print(f"    {sep}")

        for cs in changesets:
            preview_items = list(cs["deltaPayload"].items())[:3]
            preview = ", ".join(f"{k}={json.dumps(v)}" for k, v in preview_items)
            if len(cs["deltaPayload"]) > 3:
                preview += " ..."
            fields_str = ", ".join(cs["changedFields"])
            print(f"    {cs['entityId']:<12}  {cs['changeType']:<10}  {fields_str:<35}  {preview}")

        print()


# ═══════════════════════════════════════════════════════════════════════════════
# SNS PUBLISHER
# Reads manifest → publishes per-entity events + one batch-manifest event
# ═══════════════════════════════════════════════════════════════════════════════

class SNSPublisher:

    def __init__(self, infra: AWSInfrastructure):
        self.infra     = infra
        self.published = []

    def publish(self, manifest: dict):
        step_header("PUBLISH CHANGESETS → SNS")
        bid    = manifest["batchId"]
        ctype  = manifest["changeType"]
        etype  = manifest["entityType"]
        date   = manifest["batchDate"]
        ct_dir = ctype.lower() + "s"

        n = manifest["stats"]["totalRecords"]
        print(arrow(f"Publishing {n} entity events  +  1 batch manifest event"))
        print(arrow(f"Using SNS PublishBatch (10 msgs/call) to minimise API calls\n"))

        # ── Per-entity ENTITY_CHANGESET events ───────────────────────────────
        for cs in manifest["changesets"]:
            eid = cs["entityId"]
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
                    "deltaRef":     f"s3://{BUCKET}/changesets/{etype}/{date}/{bid}/{ct_dir}/{eid}_delta.json",
                    "fullAuditRef": f"s3://{BUCKET}/changesets/{etype}/{date}/{bid}/{ct_dir}/{eid}_audit.json",
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
                    "eventType":   {"DataType": "String", "StringValue": "ENTITY_CHANGESET"},
                    "changeType":  {"DataType": "String", "StringValue": ctype},
                    "entityType":  {"DataType": "String", "StringValue": etype},
                    "batchId":     {"DataType": "String", "StringValue": bid},
                    "eventVersion":{"DataType": "String", "StringValue": "1.0"},
                },
            )
            mid = resp["MessageId"]
            print(ok(f"[ENTITY_CHANGESET / {ctype:6s}]  {eid}  MessageId={mid[:8]}..."))
            self.published.append(event)

        # ── BATCH_MANIFEST event (analytics consumer only) ────────────────────
        manifest_event = {
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
            Message=json.dumps(manifest_event),
            # Note: no changeType attribute — analytics filter only checks eventType
            MessageAttributes={
                "eventType":  {"DataType": "String", "StringValue": "BATCH_MANIFEST"},
                "entityType": {"DataType": "String", "StringValue": etype},
                "batchId":    {"DataType": "String", "StringValue": bid},
            },
        )
        print(ok(f"[BATCH_MANIFEST      ]  batchId={bid}  MessageId={resp['MessageId'][:8]}..."))


# ═══════════════════════════════════════════════════════════════════════════════
# STEP FUNCTIONS ORCHESTRATOR
# Chains states in order; each state is a method
# ═══════════════════════════════════════════════════════════════════════════════

class StepFunctionsWorkflow:

    def __init__(self, infra: AWSInfrastructure):
        self.infra = infra
        self.emr   = EMRProcessor(infra)
        self.pub   = SNSPublisher(infra)

    def execute(self, batch: dict, data_key: str, audit_key: str) -> dict:
        exec_id = f"arn:aws:states:us-east-1:123456789012:execution:changeset-sfn:{uuid.uuid4()}"
        print(arrow(f"ExecutionArn: {exec_id[:72]}..."))

        self._validate_pair(data_key, audit_key)
        job_id = self._create_emr(batch["batchId"])
        self._mapping_step(batch)
        manifest = self.emr.process(batch, data_key, audit_key)
        self._terminate_emr(job_id)
        self.pub.publish(manifest)

        return manifest

    def _validate_pair(self, data_key: str, audit_key: str):
        step_header("ValidatePair  ← NEW STATE")
        for key, label in [(data_key, "data file"), (audit_key, "audit file")]:
            try:
                self.infra.s3.head_object(Bucket=BUCKET, Key=key)
                print(ok(f"{label} present: s3://{BUCKET}/{key}"))
            except Exception:
                print(err(f"{label} MISSING — CloudWatch alarm firing, Step Function FAILED"))
                raise RuntimeError(f"Pair validation failed: {key} not found")
        print(ok("naming convention valid"))
        print(ok("ValidatePair → PASSED"))

    def _create_emr(self, batch_id: str) -> str:
        step_header("CreateEMRCluster  ← REUSED")
        job_id = f"j-MOCK{batch_id[-3:].upper()}"
        print(ok(f"EMR Serverless application ID: {job_id}"))
        print(ok("Executor capacity: 4 vCPU / 16 GB × 2"))
        return job_id

    def _mapping_step(self, batch: dict):
        step_header("MappingStep  ← REUSED (enrichment)")
        print(ok("tier normalised:    BRONZE→1  SILVER→2  GOLD→3  PLATINUM→4"))
        print(ok("country normalised: CA→CAN  US→USA  UK→GBR"))
        print(ok("MappingStep → PASSED"))

    def _terminate_emr(self, job_id: str):
        step_header("TerminateEMR  ← REUSED")
        print(ok(f"Job {job_id} completed — cluster terminated"))


# ═══════════════════════════════════════════════════════════════════════════════
# CONSUMERS
# Each consumer drains its SQS queue and processes messages
# ═══════════════════════════════════════════════════════════════════════════════

class BaseConsumer:

    ICON  = "◆"
    LABEL = "Consumer"

    def __init__(self, queue_name: str, infra: AWSInfrastructure):
        self.name    = queue_name
        self.infra   = infra
        self.q_url   = infra.queues[queue_name]["url"]
        self.q_label = infra.queues[queue_name]["label"]
        self.msgs    = []

    def drain(self) -> list[dict]:
        """Pull all messages from SQS, unwrap SNS envelope, delete from queue."""
        messages = []
        while True:
            resp  = self.infra.sqs.receive_message(
                QueueUrl=self.q_url, MaxNumberOfMessages=10, WaitTimeSeconds=0
            )
            batch = resp.get("Messages", [])
            if not batch:
                break
            for m in batch:
                # SNS wraps payload in an envelope; unwrap it
                body  = json.loads(m["Body"])
                event = json.loads(body.get("Message", m["Body"]))
                messages.append(event)
                self.infra.sqs.delete_message(
                    QueueUrl=self.q_url, ReceiptHandle=m["ReceiptHandle"]
                )
        self.msgs = messages
        return messages

    def process_all(self):
        raise NotImplementedError


class MicroserviceConsumer(BaseConsumer):
    ICON  = "🔧"
    LABEL = "MICROSERVICE CONSUMER"

    def process_all(self):
        msgs = self.drain()
        print(f"\n  {BOLD}{MAGENTA}{self.ICON} {self.LABEL}{RESET}  "
              f"({len(msgs)} messages — {self.q_label})")
        for m in msgs:
            ctype  = m.get("changeType", "?")
            eid    = m.get("entityId", "?")
            fields = ", ".join(m.get("changeset", {}).get("changedFields", []))
            print(f"    [{ctype:6s}] {eid}  changed: {fields or 'n/a'}")
            print(f"           {DIM}→ fetch payload from {m['changeset']['deltaRef']}{RESET}")


class SearchConsumer(BaseConsumer):
    ICON  = "🔍"
    LABEL = "SEARCH INDEX CONSUMER"

    def process_all(self):
        msgs = self.drain()
        print(f"\n  {BOLD}{MAGENTA}{self.ICON} {self.LABEL}{RESET}  "
              f"({len(msgs)} messages — {self.q_label})")
        for m in msgs:
            ctype  = m.get("changeType", "?")
            eid    = m.get("entityId", "?")
            fields = ", ".join(m.get("changeset", {}).get("changedFields", []))
            print(f"    [{ctype:6s}] {eid}  re-indexing fields: [{fields}]")
        if not msgs:
            print(f"    {DIM}(no messages — DELETE events filtered out by SNS policy){RESET}")


class AuditConsumer(BaseConsumer):
    ICON  = "📋"
    LABEL = "AUDIT STORE CONSUMER"

    def process_all(self):
        msgs = self.drain()
        print(f"\n  {BOLD}{MAGENTA}{self.ICON} {self.LABEL}{RESET}  "
              f"({len(msgs)} messages — {self.q_label})")
        for m in msgs:
            ctype = m.get("changeType", "?")
            eid   = m.get("entityId", "?")
            ts    = m.get("processedAt", "?")[:19]
            eid_s = m.get("eventId", "?")[:8]
            print(f"    [{ctype:6s}] {eid}  eventId={eid_s}...  ts={ts}  → written to audit store")


class AnalyticsConsumer(BaseConsumer):
    ICON  = "📊"
    LABEL = "ANALYTICS CONSUMER"

    def process_all(self):
        msgs = self.drain()
        print(f"\n  {BOLD}{MAGENTA}{self.ICON} {self.LABEL}{RESET}  "
              f"({len(msgs)} batch manifests — {self.q_label})")

        col_w = [32, 12, 8, 9, 9, 9]
        headers = ["batchId", "changeType", "total", "inserts", "updates", "deletes"]
        sep     = "─" * 82

        print(f"\n    {BOLD}", end="")
        for h, w in zip(headers, col_w):
            print(f"{h:<{w}}", end="")
        print(RESET)
        print(f"    {sep}")

        for m in msgs:
            s = m.get("stats", {})
            row = [m.get("batchId","?"), m.get("changeType","?"),
                   s.get("totalRecords",0), s.get("inserts",0),
                   s.get("updates",0), s.get("deletes",0)]
            print("    " + "".join(f"{str(v):<{w}}" for v, w in zip(row, col_w)))

        print(f"\n    {DIM}→ bulk-reading processed files from S3 for Redshift COPY{RESET}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

@mock_aws
def run_simulation():
    width = 68
    print(f"\n{BOLD}{'═' * width}")
    print(f"  CHANGESET CAPTURE & NOTIFICATION SYSTEM")
    print(f"  Local Mock Simulation  ·  date={DATE}  ·  {len(BATCHES)} batches")
    print(f"{'═' * width}{RESET}")

    # 1. Provision mocked AWS resources
    infra = AWSInfrastructure()
    infra.setup()

    sfn           = StepFunctionsWorkflow(infra)
    sftp          = SFTPIngestor(infra)
    all_manifests = []

    # 2. Process each batch
    for i, batch in enumerate(BATCHES, 1):
        n = len(batch["data_lines"])
        section(
            f"BATCH {i}/{len(BATCHES)}  ▶  {batch['changeType']}",
            f"batchId={batch['batchId']}  ·  {n} entit{'y' if n == 1 else 'ies'}",
        )

        # EventBridge Scheduler → Lambda SFTP Puller → S3
        data_key, audit_key = sftp.ingest(batch)

        # EventBridge Rule detects audit file → Step Functions
        print()
        step_header("Step Functions  StartExecution")
        manifest = sfn.execute(batch, data_key, audit_key)
        all_manifests.append(manifest)

    # 3. Consumers drain their SQS queues
    section("CONSUMER PROCESSING", "draining SQS queues")

    consumers = [
        MicroserviceConsumer("microservice-consumer", infra),
        SearchConsumer("search-consumer", infra),
        AuditConsumer("audit-consumer", infra),
        AnalyticsConsumer("analytics-consumer", infra),
    ]
    for c in consumers:
        c.process_all()

    # 4. S3 bucket inventory
    section("S3 BUCKET CONTENTS", f"s3://{BUCKET}")
    objs = infra.s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    groups: dict[str, list] = {}
    for o in objs:
        parts  = o["Key"].split("/")
        prefix = f"{parts[0]}/{parts[1]}" if len(parts) > 1 else parts[0]
        groups.setdefault(prefix, []).append(o)

    for prefix, items in sorted(groups.items()):
        total_bytes = sum(o["Size"] for o in items)
        print(f"  {CYAN}{prefix}/{RESET}  ({len(items)} objects, {total_bytes:,} bytes)")
        for o in sorted(items, key=lambda x: x["Key"])[:3]:
            fname = o["Key"].split("/")[-1]
            print(f"    {DIM}└─ {fname}  ({o['Size']} B){RESET}")
        if len(items) > 3:
            print(f"    {DIM}   … and {len(items) - 3} more{RESET}")

    # 5. SNS routing summary
    section("SNS → SQS ROUTING SUMMARY")
    col_w = [38, 10, 60]
    headers = ["Queue", "Received", "Filter policy applied"]
    print(f"  {BOLD}", end="")
    for h, w in zip(headers, col_w):
        print(f"{h:<{w}}", end="")
    print(RESET)
    print("  " + "─" * 90)

    routing = [
        ("microservice-consumer", "all INSERT + UPDATE + DELETE  (eventType=ENTITY_CHANGESET)"),
        ("search-consumer",       "INSERT + UPDATE only           (eventType=ENTITY_CHANGESET, changeType in [INSERT,UPDATE])"),
        ("audit-consumer",        "all INSERT + UPDATE + DELETE  (eventType=ENTITY_CHANGESET)"),
        ("analytics-consumer",    "3 batch manifests only         (eventType=BATCH_MANIFEST)"),
    ]
    counts = {name: len(c.msgs) for c, (name, _) in zip(consumers, routing)}
    for name, desc in routing:
        n = counts[name]
        print(f"  {name:<38}{n:<10}{DIM}{desc}{RESET}")

    # 6. Final summary
    total_entities = sum(m["stats"]["totalRecords"] for m in all_manifests)
    inserts = sum(m["stats"]["inserts"] for m in all_manifests)
    updates = sum(m["stats"]["updates"] for m in all_manifests)
    deletes = sum(m["stats"]["deletes"] for m in all_manifests)
    total_events = total_entities + len(BATCHES)  # entity events + batch manifests

    section("SIMULATION COMPLETE ✓")
    print(f"  Batches processed:   {len(BATCHES)}")
    print(f"  Entities:            {total_entities}  "
          f"(inserts={inserts}  updates={updates}  deletes={deletes})")
    print(f"  SNS events published:{total_events}  "
          f"({total_entities} entity + {len(BATCHES)} batch manifests)")
    print(f"  S3 objects written:  {len(objs)}")
    print(f"\n  {YELLOW}Estimated real AWS cost for this run: ~$0.83{RESET}  "
          f"{DIM}(moto is free!){RESET}")
    print()


if __name__ == "__main__":
    run_simulation()
