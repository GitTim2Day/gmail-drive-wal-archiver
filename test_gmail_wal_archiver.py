#!/usr/bin/env python3
"""Mocked state logic validation suite for gmail_wal_archiver.py."""

import base64
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from gmail_wal_archiver import (
    GmailWalArchiver,
    WalStore,
    decode_gmail_base64url,
    route_attachment,
    safe_filename,
    safe_slug,
    sha256_hex,
)


class TestHelpers(unittest.TestCase):
    def test_safe_slug(self):
        self.assertEqual(safe_slug("Bounded Ratio Test"), "bounded_ratio_test")
        self.assertEqual(safe_slug(""), "untitled")

    def test_safe_filename_sanitizes_backslash(self):
        self.assertEqual(safe_filename(r"a\b.py"), "a_b.py")

    def test_base64url_padding_decode(self):
        raw = b"hello"
        encoded = base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")
        self.assertEqual(decode_gmail_base64url(encoded), raw)

    def test_sha256_hex(self):
        self.assertEqual(
            sha256_hex(b"hello"),
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
        )


class TestWalStore(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.wal_path = Path(self.temp_dir) / "manifest.jsonl"
        self.wal = WalStore(self.wal_path)

    def tearDown(self):
        if self.wal_path.exists():
            self.wal_path.unlink()
        tmp = self.wal_path.with_suffix(self.wal_path.suffix + ".tmp")
        if tmp.exists():
            tmp.unlink()
        os.rmdir(self.temp_dir)

    def test_identity_key_is_unique_and_deterministic(self):
        key1 = WalStore.identity_key("msg123", 0, "file.zip", "abc123def456")
        key2 = WalStore.identity_key("msg123", 0, "file.zip", "abc123def456")
        key3 = WalStore.identity_key("msg123", 1, "file.zip", "abc123def456")
        self.assertEqual(key1, key2)
        self.assertNotEqual(key1, key3)

    def test_wal_prevents_duplicate_via_identity_key(self):
        record = {
            "message_id": "msg001",
            "attachment_index": 0,
            "original_filename": "test.vox",
            "sha256": "deadbeef1234",
        }
        self.wal.append(record)
        existing = self.wal.lookup("msg001", 0, "test.vox", "deadbeef1234")
        self.assertIsNotNone(existing)

    def test_wal_loads_and_persists_atomic_append(self):
        record = {
            "message_id": "msg002",
            "attachment_index": 5,
            "original_filename": "brain.nii",
            "sha256": "cafe1234",
            "sheets_synced": False,
        }
        self.wal.append(record)
        new_wal = WalStore(self.wal_path)
        self.assertEqual(len(new_wal.records), 1)
        self.assertIn("cafe1234", new_wal.sha_index)

    def test_mark_synced_rewrites_wal(self):
        record = {
            "message_id": "msg003",
            "attachment_index": 0,
            "original_filename": "a.py",
            "sha256": "abc",
            "sheets_synced": False,
        }
        self.wal.append(record)
        key = WalStore.identity_key("msg003", 0, "a.py", "abc")
        self.wal.mark_synced({key})
        self.assertTrue(self.wal.lookup("msg003", 0, "a.py", "abc")["sheets_synced"])


class TestRouting(unittest.TestCase):
    def test_route_attachment_pure_structural(self):
        self.assertEqual(route_attachment("archive.zip", "application/zip"), "packages")
        self.assertEqual(route_attachment("brain_scan.vox", "image/voxel"), "media_voxel")
        self.assertEqual(route_attachment("scan.dcm", ""), "medical")
        self.assertEqual(route_attachment("model.kbl9", ""), "core")
        self.assertEqual(route_attachment("report.pdf", ""), "research")
        self.assertEqual(route_attachment("unknown.unknown", ""), "inbox")


class TestGmailWalArchiverMocked(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.wal_path = Path(self.temp_dir) / "manifest.jsonl"
        self.mock_gmail = MagicMock()
        self.mock_drive = MagicMock()
        self.mock_sheets = MagicMock()
        self.archiver = GmailWalArchiver(
            gmail=self.mock_gmail,
            drive=self.mock_drive,
            sheets=self.mock_sheets,
            spreadsheet_id="test_sheet_123",
            wal_path=self.wal_path,
        )
        self.archiver.folder_ids = {
            "local_audit": "folder_local_audit",
            "core": "folder_core",
            "media_voxel": "folder_media_voxel",
            "research": "folder_research",
            "medical": "folder_medical",
            "packages": "folder_packages",
            "inbox": "folder_inbox",
        }

    def tearDown(self):
        if self.wal_path.exists():
            self.wal_path.unlink()
        tmp = self.wal_path.with_suffix(self.wal_path.suffix + ".tmp")
        if tmp.exists():
            tmp.unlink()
        os.rmdir(self.temp_dir)

    def _mock_gmail_attachment(self, data: bytes) -> None:
        self.mock_gmail.users().messages().attachments().get().execute.return_value = {
            "data": base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")
        }

    def _mock_sheets_success(self) -> None:
        self.mock_sheets.spreadsheets().values().append().execute.return_value = {}

    def test_atomic_rollback_on_wal_failure(self):
        self._mock_gmail_attachment(b"rollback payload")
        self.mock_drive.files().create().execute.return_value = {"id": "drive_orphan_999"}
        with patch.object(self.archiver.wal, "append", side_effect=IOError("Disk full")):
            ok, state = self.archiver.commit_attachment(
                thread_id="t123",
                message_id="m001",
                attachment_index=0,
                attachment_part={"filename": "test.vox", "body": {"attachmentId": "att1"}},
                message_date_utc="2026-06-10T12:00:00Z",
            )
        self.assertFalse(ok)
        self.assertEqual(state, "WAL_APPEND_FAILED")
        self.mock_drive.files().delete.assert_called_once_with(fileId="drive_orphan_999")

    def test_full_success_path_no_reprocessing(self):
        self._mock_gmail_attachment(b"dummy voxel data")
        self._mock_sheets_success()
        self.mock_drive.files().create().execute.return_value = {"id": "drive_ok_777"}
        self.mock_drive.files().get().execute.return_value = {"id": "drive_ok_777"}
        ok, state = self.archiver.commit_attachment(
            thread_id="t999",
            message_id="m002",
            attachment_index=0,
            attachment_part={"filename": "voxel.mesh", "body": {"attachmentId": "attX"}},
            message_date_utc="2026-06-10T14:30:00Z",
        )
        self.assertTrue(ok)
        self.assertEqual(state, "NEWLY_ARCHIVED_VERIFIED")
        ok2, state2 = self.archiver.commit_attachment(
            thread_id="t999",
            message_id="m002",
            attachment_index=0,
            attachment_part={"filename": "voxel.mesh", "body": {"attachmentId": "attX"}},
            message_date_utc="2026-06-10T14:30:00Z",
        )
        self.assertTrue(ok2)
        self.assertEqual(state2, "ALREADY_ARCHIVED_VERIFIED")

    def test_sheets_sync_failure_blocks_success(self):
        self._mock_gmail_attachment(b"sheet failure")
        self.mock_drive.files().create().execute.return_value = {"id": "drive_ok_888"}
        self.mock_sheets.spreadsheets().values().append().execute.side_effect = RuntimeError("sheet down")
        ok, state = self.archiver.commit_attachment(
            thread_id="t_sheet",
            message_id="m_sheet",
            attachment_index=0,
            attachment_part={"filename": "data.json", "body": {"attachmentId": "attS"}},
            message_date_utc="2026-06-10T14:30:00Z",
        )
        self.assertFalse(ok)
        self.assertEqual(state, "SHEETS_SYNC_FAILED")

    def test_wal_with_missing_drive_file_fails(self):
        record = {
            "message_id": "m_missing",
            "thread_id": "t_missing",
            "attachment_index": 0,
            "original_filename": "a.py",
            "saved_filename": "a.py",
            "mime_type": "text/x-python",
            "size_bytes": 3,
            "sha256": sha256_hex(b"abc"),
            "routing_target": "02_KBLD_KBL9_Core",
            "drive_file_id": "missing_drive",
            "message_date_utc": "2026-06-10T00:00:00Z",
            "archived_at_utc": "2026-06-10T00:00:01Z",
            "state": "DRIVE_FILE_WRITTEN_WAL_APPENDED",
            "sheets_synced": True,
        }
        self.archiver.wal.append(record)
        self._mock_gmail_attachment(b"abc")
        self.mock_drive.files().get().execute.side_effect = RuntimeError("not found")
        ok, state = self.archiver.commit_attachment(
            thread_id="t_missing",
            message_id="m_missing",
            attachment_index=0,
            attachment_part={"filename": "a.py", "body": {"attachmentId": "attM"}},
            message_date_utc="2026-06-10T00:00:00Z",
        )
        self.assertFalse(ok)
        self.assertEqual(state, "WAL_WITH_MISSING_DRIVE_FILE")


if __name__ == "__main__":
    unittest.main(verbosity=2)
