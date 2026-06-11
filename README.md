# Gmail Drive WAL Archiver

Status: Candidate build.

Validation: local mocked unit tests are included. Live Gmail / Drive / Sheets validation is pending.

This repository contains a manifest-backed Gmail attachment recovery tool. It archives Gmail attachments to Google Drive using deterministic filenames, SHA-256 identity, a local JSONL write-ahead log, Google Sheets replay, and Gmail thread labeling only after verified attachment recovery.

## Local validation

```bash
python -m pip install -r requirements.txt
python -m compileall gmail_wal_archiver.py test_gmail_wal_archiver.py
python -m unittest test_gmail_wal_archiver.py -v
```

## Current scope

This is a candidate recovery build. It is not a production-complete Gmail archive system until live Gmail / Drive / Sheets validation has been performed on a bounded test window.
