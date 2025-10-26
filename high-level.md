# High-level design

Components:
- **Server**: single RPC server that stores files on disk and exposes RPCs: `open/create/read/write/close`, also? `get_metadata/get_checksum/`?
- **Client**: requests whole-file on first `open`, caches locally (temp file). All subsequent `read`/`write` happen on the local copy. On `close`, client performs `TestAuth` (send locally computed checksum) to server; if server says file is unchanged, client sends updated file (flush). If server says changed, client resolves conflict (choose server copy / merge / error).
- **Metadata DB (server-side)**: for each file store: checksum (eg SHA256), modification timestamp, version number and optional lock/lease info.
- **Fault tolerance**: minimal techniques:
    - Server writes updates with write-ahead log (WAL) + atomic file replace (write to temp + rename).
    - Clients retry RPCs with exponential backoff.
    - Optional: implement a hot-standby backup server that pulls WAL entries periodically (simple replication).
    - Lease-based locks to avoid concurrent writers.


