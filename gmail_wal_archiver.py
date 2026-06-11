#!/usr/bin/env python3
"""
gmail_wal_archiver.py

Manifest-backed Gmail attachment archiver.

Core rules:
- No Gmail deletion.
- No deletion of pre-existing archive files.
- Newly-created Drive files may be deleted only as rollback if WAL append fails.
- Local JSONL WAL is the primary recovery ledger.
- Attachment identity = message_id + attachment_index + original_filename + sha256.
- Drive file creation happens before WAL append.
- Google Sheets sync is replayable from unsynced WAL rows.
- Gmail thread gets KBLD_ARCHIVED only after every discovered attachment reaches a verified terminal state.
"""

import argparse
import base64
import hashlib
import io
import json
import logging
import mimetypes
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

DEFAULT_ARCHIVE_LABEL = "KBLD_ARCHIVED"
DEFAULT_MASTER_FOLDER = "Timothy H. Norman - Project Archive"

FOLDER_TOPOLOGY = {
    "local_audit": "01_KBLD_Local_Audit_App",
    "core": "02_KBLD_KBL9_Core",
    "media_voxel": "03_Media_Voxel_Assets",
    "research": "04_Research_Docs",
    "medical": "05_Medical_Data_Vault",
    "packages": "06_Package_Archives",
    "inbox": "07_General_Project_Inbox",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def gmail_internal_date_to_iso(internal_date_ms: str) -> str:
    try:
        ts = int(internal_date_ms) / 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return ""


def decode_gmail_base64url(data: str) -> bytes:
    data = data or ""
    data += "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data.encode("utf-8"))


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def safe_slug(value: str, max_len: int = 40) -> str:
    value = (value or "").lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = value.strip("_")
    if not value:
        value = "untitled"
    return value[:max_len]


def safe_filename(value: str) -> str:
    value = value or "unnamed_attachment"
    return re.sub(r'[\\/:*?"<>|#%{}~&]', "_", value)


def short_hash(value: str, n: int = 12) -> str:
    return value[:n]


def route_attachment(filename: str, mime_type: str) -> str:
    name = (filename or "").lower()
    mime = (mime_type or "").lower()
    if name.endswith((".zip", ".tar", ".tar.gz", ".tgz", ".gz", ".7z", ".rar", ".sha256")):
        return "packages"
    if "zip" in mime or "gzip" in mime or "compressed" in mime:
        return "packages"
    if name.endswith((".dcm", ".dicom", ".nii", ".nii.gz")):
        return "medical"
    if "dicom" in mime or "nifti" in mime or "medical" in mime:
        return "medical"
    if name.endswith((".vox", ".mesh")):
        return "media_voxel"
    if mime.startswith("image/"):
        return "media_voxel"
    if name.endswith((".py", ".sh", ".json", ".jsonl", ".ndjson", ".kbld", ".kbl9")):
        return "core"
    if name.endswith((".pdf", ".docx", ".doc", ".txt", ".md", ".csv", ".xlsx", ".xls")):
        return "research"
    if name.endswith((".html", ".htm")):
        return "local_audit"
    return "inbox"


def iter_gmail_parts(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    stack = [payload]
    while stack:
        part = stack.pop()
        yield part
        for child in part.get("parts", []) or []:
            stack.append(child)


def extract_attachment_parts(message: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = message.get("payload", {}) or {}
    out: List[Dict[str, Any]] = []
    for part in iter_gmail_parts(payload):
        filename = part.get("filename") or ""
        body = part.get("body", {}) or {}
        attachment_id = body.get("attachmentId")
        if filename and attachment_id:
            out.append(part)
    return out


class WalStore:
    def __init__(self, path: Path):
        self.path = path
        self.records: List[Dict[str, Any]] = []
        self.identity_index: Dict[str, Dict[str, Any]] = {}
        self.sha_index: Set[str] = set()
        self.load()

    @staticmethod
    def identity_key(message_id: str, attachment_index: int, original_filename: str, sha256: str) -> str:
        return f"{message_id}::{attachment_index}::{original_filename}::{sha256}"

    def load(self) -> None:
        self.records = []
        self.identity_index = {}
        self.sha_index = set()
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logging.warning("Skipping invalid WAL JSON line %s", line_num)
                    continue
                self.records.append(record)
                key = self.identity_key(str(record.get("message_id", "")), int(record.get("attachment_index", -1)), str(record.get("original_filename", "")), str(record.get("sha256", "")))
                self.identity_index[key] = record
                sha = record.get("sha256")
                if sha:
                    self.sha_index.add(str(sha))

    def lookup(self, message_id: str, attachment_index: int, original_filename: str, sha256: str) -> Optional[Dict[str, Any]]:
        return self.identity_index.get(self.identity_key(message_id, attachment_index, original_filename, sha256))

    def append(self, record: Dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())
        self.records.append(record)
        key = self.identity_key(str(record["message_id"]), int(record["attachment_index"]), str(record["original_filename"]), str(record["sha256"]))
        self.identity_index[key] = record
        self.sha_index.add(str(record["sha256"]))

    def unsynced_records(self) -> List[Dict[str, Any]]:
        return [r for r in self.records if not r.get("sheets_synced")]

    def mark_synced(self, synced_identity_keys: Set[str]) -> None:
        if not synced_identity_keys:
            return
        changed = False
        synced_at = utc_now_iso()
        for record in self.records:
            key = self.identity_key(str(record.get("message_id", "")), int(record.get("attachment_index", -1)), str(record.get("original_filename", "")), str(record.get("sha256", "")))
            if key in synced_identity_keys and not record.get("sheets_synced"):
                record["sheets_synced"] = True
                record["sheets_synced_at_utc"] = synced_at
                changed = True
        if changed:
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            with tmp.open("w", encoding="utf-8") as f:
                for record in self.records:
                    f.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
                f.flush()
                os.fsync(f.fileno())
            tmp.replace(self.path)
            self.load()


class GmailWalArchiver:
    def __init__(self, gmail: Resource, drive: Resource, sheets: Resource, spreadsheet_id: str, wal_path: Path, master_folder_name: str = DEFAULT_MASTER_FOLDER, archive_label_name: str = DEFAULT_ARCHIVE_LABEL):
        self.gmail = gmail
        self.drive = drive
        self.sheets = sheets
        self.spreadsheet_id = spreadsheet_id
        self.wal = WalStore(wal_path)
        self.master_folder_name = master_folder_name
        self.archive_label_name = archive_label_name
        self.master_folder_id: Optional[str] = None
        self.folder_ids: Dict[str, str] = {}
        self.archive_label_id: Optional[str] = None

    def resolve_or_create_folder(self, name: str, parent_id: Optional[str] = None) -> str:
        escaped_name = name.replace("'", "\\'")
        q = f"name = '{escaped_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        if parent_id:
            q += f" and '{parent_id}' in parents"
        response = self.drive.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=10).execute()
        files = response.get("files", [])
        if files:
            return files[0]["id"]
        body = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
        if parent_id:
            body["parents"] = [parent_id]
        created = self.drive.files().create(body=body, fields="id").execute()
        return created["id"]

    def initialize_drive_topology(self) -> None:
        self.master_folder_id = self.resolve_or_create_folder(self.master_folder_name)
        for key, folder_name in FOLDER_TOPOLOGY.items():
            self.folder_ids[key] = self.resolve_or_create_folder(folder_name, parent_id=self.master_folder_id)

    def ensure_archive_label(self) -> str:
        if self.archive_label_id:
            return self.archive_label_id
        response = self.gmail.users().labels().list(userId="me").execute()
        for label in response.get("labels", []):
            if label.get("name", "").lower() == self.archive_label_name.lower():
                self.archive_label_id = label["id"]
                return self.archive_label_id
        body = {"name": self.archive_label_name, "labelListVisibility": "labelShow", "messageListVisibility": "show"}
        created = self.gmail.users().labels().create(userId="me", body=body).execute()
        self.archive_label_id = created["id"]
        return self.archive_label_id

    def sync_unsynced_wal_to_sheets(self) -> bool:
        unsynced = self.wal.unsynced_records()
        if not unsynced:
            return True
        rows = []
        keys = set()
        for r in unsynced:
            rows.append([r.get("message_id", ""), r.get("thread_id", ""), r.get("attachment_index", ""), r.get("original_filename", ""), r.get("saved_filename", ""), r.get("mime_type", ""), r.get("size_bytes", ""), r.get("sha256", ""), r.get("routing_target", ""), r.get("drive_file_id", ""), r.get("message_date_utc", ""), r.get("archived_at_utc", ""), r.get("state", "")])
            keys.add(WalStore.identity_key(str(r.get("message_id", "")), int(r.get("attachment_index", -1)), str(r.get("original_filename", "")), str(r.get("sha256", ""))))
        try:
            self.sheets.spreadsheets().values().append(spreadsheetId=self.spreadsheet_id, range="Sheet1!A:M", valueInputOption="RAW", insertDataOption="INSERT_ROWS", body={"values": rows}).execute()
            self.wal.mark_synced(keys)
            logging.info("SHEETS_SYNC_OK records=%s", len(rows))
            return True
        except Exception as e:
            logging.error("SHEETS_SYNC_FAILED %s", e)
            return False

    def list_threads(self, search_query: str) -> List[Dict[str, Any]]:
        all_threads: List[Dict[str, Any]] = []
        page_token = None
        while True:
            kwargs = {"userId": "me", "q": search_query, "maxResults": 100}
            if page_token:
                kwargs["pageToken"] = page_token
            response = self.gmail.users().threads().list(**kwargs).execute()
            all_threads.extend(response.get("threads", []))
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return all_threads

    def download_attachment_bytes(self, message_id: str, attachment_id: str) -> bytes:
        response = self.gmail.users().messages().attachments().get(userId="me", messageId=message_id, id=attachment_id).execute()
        return decode_gmail_base64url(response.get("data", ""))

    def deterministic_saved_filename(self, message_date_utc: str, message_id: str, attachment_index: int, original_filename: str, sha256: str) -> str:
        date_part = "unknown_date"
        try:
            dt = datetime.fromisoformat(message_date_utc.replace("Z", "+00:00"))
            date_part = dt.strftime("%Y%m%d_%H%M%S")
        except Exception:
            pass
        safe_original = safe_filename(original_filename)
        return f"{date_part}__msg_{message_id}__att_{attachment_index}__sha_{short_hash(sha256)}__{safe_original}"

    def drive_file_exists(self, file_id: str) -> bool:
        if not file_id:
            return False
        try:
            self.drive.files().get(fileId=file_id, fields="id").execute()
            return True
        except Exception:
            return False

    def commit_attachment(self, thread_id: str, message_id: str, attachment_index: int, attachment_part: Dict[str, Any], message_date_utc: str) -> Tuple[bool, str]:
        original_filename = attachment_part.get("filename") or "unnamed_attachment"
        body = attachment_part.get("body", {}) or {}
        attachment_id = body.get("attachmentId")
        mime_type = attachment_part.get("mimeType") or mimetypes.guess_type(original_filename)[0] or "application/octet-stream"
        if not attachment_id:
            return False, "MISSING_ATTACHMENT_ID"
        try:
            data = self.download_attachment_bytes(message_id, attachment_id)
        except Exception as e:
            logging.error("GMAIL_DOWNLOAD_FAILED message=%s file=%s error=%s", message_id, original_filename, e)
            return False, "GMAIL_DOWNLOAD_FAILED"
        sha = sha256_hex(data)
        existing = self.wal.lookup(message_id=message_id, attachment_index=attachment_index, original_filename=original_filename, sha256=sha)
        if existing:
            if not self.drive_file_exists(str(existing.get("drive_file_id", ""))):
                return False, "WAL_WITH_MISSING_DRIVE_FILE"
            if not existing.get("sheets_synced"):
                if not self.sync_unsynced_wal_to_sheets():
                    return False, "WAL_EXISTS_BUT_SHEETS_SYNC_FAILED"
            return True, "ALREADY_ARCHIVED_VERIFIED"
        route_key = route_attachment(original_filename, mime_type)
        folder_id = self.folder_ids.get(route_key)
        if not folder_id:
            return False, "ROUTE_FOLDER_NOT_INITIALIZED"
        saved_filename = self.deterministic_saved_filename(message_date_utc, message_id, attachment_index, original_filename, sha)
        try:
            media = MediaIoBaseUpload(io.BytesIO(data), mimetype=mime_type, resumable=True)
            created = self.drive.files().create(body={"name": saved_filename, "parents": [folder_id]}, media_body=media, fields="id").execute()
            drive_file_id = created["id"]
        except Exception as e:
            logging.error("DRIVE_WRITE_FAILED message=%s file=%s error=%s", message_id, original_filename, e)
            return False, "DRIVE_WRITE_FAILED"
        record = {"message_id": message_id, "thread_id": thread_id, "attachment_index": attachment_index, "original_filename": original_filename, "saved_filename": saved_filename, "mime_type": mime_type, "size_bytes": len(data), "sha256": sha, "routing_target": FOLDER_TOPOLOGY[route_key], "drive_file_id": drive_file_id, "message_date_utc": message_date_utc, "archived_at_utc": utc_now_iso(), "state": "DRIVE_FILE_WRITTEN_WAL_APPENDED", "sheets_synced": False, "sheets_synced_at_utc": ""}
        try:
            self.wal.append(record)
        except Exception as e:
            logging.critical("WAL_APPEND_FAILED message=%s file=%s error=%s", message_id, original_filename, e)
            try:
                self.drive.files().delete(fileId=drive_file_id).execute()
                logging.info("ORPHAN_ROLLBACK_OK drive_file_id=%s", drive_file_id)
            except Exception as rollback_error:
                logging.critical("ORPHAN_ROLLBACK_FAILED drive_file_id=%s error=%s", drive_file_id, rollback_error)
            return False, "WAL_APPEND_FAILED"
        if not self.sync_unsynced_wal_to_sheets():
            return False, "SHEETS_SYNC_FAILED"
        return True, "NEWLY_ARCHIVED_VERIFIED"

    def process_thread(self, thread_id: str) -> Tuple[bool, Dict[str, int]]:
        stats = {"attachments_seen": 0, "attachments_verified": 0, "attachments_failed": 0}
        thread = self.gmail.users().threads().get(userId="me", id=thread_id, format="full").execute()
        for message in thread.get("messages", []):
            message_id = message.get("id", "")
            message_date_utc = gmail_internal_date_to_iso(str(message.get("internalDate", "")))
            for attachment_index, part in enumerate(extract_attachment_parts(message)):
                stats["attachments_seen"] += 1
                ok, state = self.commit_attachment(thread_id, message_id, attachment_index, part, message_date_utc)
                logging.info("ATTACHMENT_STATE thread=%s message=%s att=%s state=%s", thread_id, message_id, attachment_index, state)
                if ok:
                    stats["attachments_verified"] += 1
                else:
                    stats["attachments_failed"] += 1
        complete = stats["attachments_seen"] > 0 and stats["attachments_failed"] == 0 and stats["attachments_seen"] == stats["attachments_verified"]
        return complete, stats

    def apply_archive_label(self, thread_id: str) -> None:
        label_id = self.ensure_archive_label()
        self.gmail.users().threads().modify(userId="me", id=thread_id, body={"addLabelIds": [label_id]}).execute()

    def run(self, search_query: str) -> int:
        self.initialize_drive_topology()
        self.ensure_archive_label()
        if not self.sync_unsynced_wal_to_sheets():
            logging.error("Startup WAL replay to Sheets failed. Aborting before Gmail changes.")
            return 2
        threads = self.list_threads(search_query)
        logging.info("THREADS_FOUND count=%s", len(threads))
        total_complete = 0
        total_failed = 0
        for item in threads:
            thread_id = item.get("id")
            if not thread_id:
                continue
            try:
                complete, stats = self.process_thread(thread_id)
                logging.info("THREAD_STATS thread=%s stats=%s complete=%s", thread_id, stats, complete)
                if complete:
                    self.apply_archive_label(thread_id)
                    total_complete += 1
                    logging.info("THREAD_MARKED_ARCHIVED thread=%s", thread_id)
                else:
                    total_failed += 1
                    logging.warning("THREAD_NOT_MARKED_ARCHIVED thread=%s", thread_id)
            except HttpError as e:
                total_failed += 1
                logging.error("THREAD_HTTP_ERROR thread=%s error=%s", thread_id, e)
            except Exception as e:
                total_failed += 1
                logging.error("THREAD_UNEXPECTED_ERROR thread=%s error=%s", thread_id, e)
        logging.info("RUN_COMPLETE complete_threads=%s failed_threads=%s", total_complete, total_failed)
        return 0 if total_failed == 0 else 1


def initialize_authenticated_services(credentials_json_path: str, token_json_path: str) -> Tuple[Resource, Resource, Resource]:
    creds = None
    if os.path.exists(token_json_path):
        creds = Credentials.from_authorized_user_file(token_json_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_json_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_json_path, "w", encoding="utf-8") as f:
            f.write(creds.to_json())
    return build("gmail", "v1", credentials=creds), build("drive", "v3", credentials=creds), build("sheets", "v4", credentials=creds)


def build_search_query(after: str, before: str, archive_label: str, email_1: str, email_2: str) -> str:
    return f"has:attachment after:{after} before:{before} (from:{email_1} OR from:{email_2} OR kbld OR kbl9 OR voxel OR token OR ev2 OR rex OR nifti OR dicom OR mesh OR bounded OR ratio OR audit) -label:{archive_label}"


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Gmail WAL attachment archiver")
    parser.add_argument("--credentials", default="credentials.json")
    parser.add_argument("--token", default="token.json")
    parser.add_argument("--spreadsheet-id", required=True)
    parser.add_argument("--wal", default="manifest.jsonl")
    parser.add_argument("--after", default="2026/06/01")
    parser.add_argument("--before", default="2026/06/11")
    parser.add_argument("--email-1", required=True)
    parser.add_argument("--email-2", required=True)
    parser.add_argument("--archive-label", default=DEFAULT_ARCHIVE_LABEL)
    parser.add_argument("--master-folder", default=DEFAULT_MASTER_FOLDER)
    parser.add_argument("--query", default="")
    args = parser.parse_args(argv)
    gmail, drive, sheets = initialize_authenticated_services(args.credentials, args.token)
    archiver = GmailWalArchiver(gmail=gmail, drive=drive, sheets=sheets, spreadsheet_id=args.spreadsheet_id, wal_path=Path(args.wal), master_folder_name=args.master_folder, archive_label_name=args.archive_label)
    query = args.query.strip() or build_search_query(args.after, args.before, args.archive_label, args.email_1, args.email_2)
    return archiver.run(query)


if __name__ == "__main__":
    sys.exit(main())
