# Known Limitations

- Mock tests validate local state logic only.
- Live Gmail / Drive / Sheets behavior has not yet been verified.
- Google API credentials and OAuth setup are required for real use.
- Google Sheets replay may duplicate rows if Sheets append succeeds but local synced-state rewrite fails.
- Broad Google API exceptions are classified coarsely.
- No Gmail message deletion is performed.
- No pre-existing archive files are deleted.
- Newly-created Drive files may be deleted only as rollback if WAL append fails.
- The first live run should use a bounded date window and manual inspection before expansion.
