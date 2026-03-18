---
name: changeset-simulation
description: Run the changeset capture & notification mock simulation and push results to GitHub
---

Run the changeset capture and notification system mock simulation.

## Context
- Project path: `/Users/valentinarodrigues/vr27/changeset-architecture`
- Simulation file: `mock/simulation.py`
- Venv: `/tmp/changeset-venv` (recreate if missing)
- GitHub repo: https://github.com/valentinarodrigues/changeset-architecture

## What this system does
End-to-end mock of an AWS changeset ingestion pipeline:
- External SFTP → S3 landing zone (Lambda puller)
- EventBridge triggers Step Functions on audit file arrival
- Step Functions: ValidatePair → CreateEMR → MappingStep → SparkJob → TerminateEMR → PublishChangesets
- EMR Spark (mocked with pandas) joins data file + audit file on entityId, extracts changed fields
- SNS publishes per-entity ENTITY_CHANGESET events + one BATCH_MANIFEST event per batch
- 4 SQS consumer queues with SNS filter policies:
  - microservice-consumer: all ENTITY_CHANGESET events
  - search-consumer: INSERT + UPDATE only
  - audit-consumer: all ENTITY_CHANGESET events
  - analytics-consumer: BATCH_MANIFEST only

## Steps to run

1. Check if venv exists and has dependencies, recreate if needed:
```bash
if [ ! -f /tmp/changeset-venv/bin/python ]; then
  python3 -m venv /tmp/changeset-venv
  /tmp/changeset-venv/bin/pip install -q -r /Users/valentinarodrigues/vr27/changeset-architecture/mock/requirements.txt
fi
```

2. Run the simulation from the mock directory:
```bash
cd /Users/valentinarodrigues/vr27/changeset-architecture/mock
/tmp/changeset-venv/bin/python simulation.py
```

3. Verify the run succeeded — look for "SIMULATION COMPLETE ✓" in the output and confirm:
   - 3 batches processed (INSERT, UPDATE, DELETE)
   - 8 entities total
   - 11 SNS events published
   - 22 S3 objects written
   - search-consumer received exactly 6 messages (DELETE filtered out)
   - analytics-consumer received exactly 3 messages (BATCH_MANIFEST only)

4. If the simulation output looks correct, commit and push any changes to GitHub:
```bash
cd /Users/valentinarodrigues/vr27/changeset-architecture
git add -A
git status
```
Only commit if there are actual changes (do not create empty commits). Use a concise commit message describing what changed (e.g. "Re-run simulation: updated mock data" or "Fix: corrected SNS filter policy").

5. Report back with:
   - Whether the simulation passed or failed
   - The SNS routing summary table (queue name, message count, filter applied)
   - The S3 object count
   - The git status (committed + pushed, or nothing to commit)
   - Any errors encountered and how they were resolved
