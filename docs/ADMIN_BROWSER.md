# Admin data browser

Status: implemented for roadmap issue #106, hardened by issue #110, and made
placement-aware by issue #57

BriskDB serves a small read-only data explorer from the existing HTTP listener.
Open `/admin` (or `/admin/`) to use the embedded application. Its HTML, CSS,
and JavaScript are compiled into the server binary; loading the page does not
contact a package CDN, font service, analytics service, or other third party.

## Temporary login

The first implementation has one exact built-in credential pair:

- username: `admin`
- password: `admin`

This is a temporary development convenience, not the user/role, password
rotation, or transport-encryption work described by roadmap issues #56 and #64.
Anyone who can reach the HTTP listener knows these credentials. Server startup
therefore restricts the current unauthenticated HTTP service to an IPv4 or IPv6
loopback address. Do not treat this login as a production identity boundary.
The existing `/health` and `/v1/*` endpoints retain their
previous behavior and are not made authenticated by issue #106.

A successful login creates an opaque session from 32 operating-system-random
bytes and sends its lowercase 64-character hexadecimal token only in the
`briskdb_admin_session` cookie. The cookie is `HttpOnly`, `SameSite=Strict`, has
`Path=/admin`, and has an absolute eight-hour lifetime (`Max-Age=28800`). It is
not persisted in the manifest, shards, or any other file. Restarting BriskDB
invalidates every browser session.

At most 128 sessions are retained in process memory. A successful login at that
capacity evicts the earliest-expiring session, with the token as a deterministic
tie-break. Logout invalidates only the presented session and always clears its
cookie; other authenticated browsers remain independent. Logout is idempotent
even when the cookie is absent or already unusable. Protected session,
discovery, and row calls with a missing, malformed, unknown, expired, or
logged-out cookie receive HTTP 401 with fixed JSON and no `Set-Cookie` header.
Only explicit logout clears the cookie. This means an older unauthorized
response cannot clear a session issued by a newer login. The application also
tracks an authentication generation so an older response cannot replace a
newer logged-in view.
Session-cookie state is separate from a core `Session`: every inspection
request still creates a short-lived engine session, and login does not create a
multi-request SQL transaction.

## Routes

| Surface | Contract |
| --- | --- |
| `GET /admin`, `GET /admin/` | Embedded application shell |
| `GET /admin/assets/styles.css` | Embedded stylesheet |
| `GET /admin/assets/logic.js` | Embedded, independently tested display and authentication-order logic |
| `GET /admin/assets/app.js` | Embedded application code |
| `POST /admin/api/login` | Accept JSON `{"username":"admin","password":"admin"}`; return `{"authenticated":true}` and set the session cookie |
| `GET /admin/api/session` | Return `{"authenticated":true,"username":"admin"}` for a live cookie; otherwise return HTTP 401 |
| `POST /admin/api/logout` | Revoke a presented live token if any, always clear the cookie, and return `{"authenticated":false}` |
| `GET /admin/api/overview` | Return the default logical database's browseable `tables`, logical `scope`, configured `shard_count`, and any discovery `visited_shards` |
| `GET /admin/api/count?table=T` | Return the exact placement-aware total as `table`, logical `scope`, sorted `visited_shards`, and `total_rows` |
| `GET /admin/api/rows?table=T&limit=L&offset=O` | Return one logical page as `table`, `scope`, sorted `visited_shards`, `ordering`, `limit`, `offset`, `has_more`, ordered `columns`, and positional `rows`; limit defaults to 50 and offset to zero |

The shell and its three assets are public so the application can render its login
state. Session, overview, count, and row calls require the server-side session check;
logout remains an idempotent cookie-clearing operation. Hiding controls in
JavaScript is not the access check.

Display conversion and authentication-order transitions live in the small
`logic.js` asset. Executable Node.js tests cover that pure logic, while a Rust
integration test syntax-checks both scripts and runs the logic suite as part of
the existing all-target CI command. Node.js is needed only for development
tests; serving the embedded browser has no frontend build step.

## Logical table view

The normal explorer has no physical-shard selector. With a populated catalog,
the overview reads authoritative metadata for the default logical database and
lists every `Sharded` or `Global` table while excluding `Catalog` placement.
With an empty catalog, compatibility discovery reads ordinary tables from
physical shard 0. SQLite-owned names beginning with `sqlite_`, the exact
BriskDB-owned name `briskdb`, names beginning with `briskdb_`, views, and other
non-table objects are never presented. Guessing an excluded, Catalog, or absent
name does not make it browseable.

Placement determines the logical table's files:

- `Sharded` visits every physical shard because each ordinary row has exactly
  one metadata-selected owner.
- `Global` visits canonical shard 0 once, even if an operator has copied the
  same data elsewhere.
- Empty-catalog compatibility visits every file and requires the table and
  column shape to match on all of them.

Before counting or paging, BriskDB verifies the exact ordinary table, column,
and local browse-order metadata on every relevant file. At most eight physical
inspections run at once. A missing table, incompatible shape or ordering,
shard failure, deadline, schema-generation change, or arithmetic overflow fails
the request; no partial count or page is returned.

## Logical pagination and exact total

Selecting a table starts a specialized exact count. BriskDB runs generated
read-only `COUNT(*)` inspections on the relevant files and checked-adds them as
an unsigned 64-bit total. A Sharded row is counted once at its owner; a Global
row is counted once on shard 0. This is an admin-specific aggregation plan, not
support for arbitrary multi-shard aggregate SQL. `total_rows` uses a direct JSON
integer through `9007199254740991`; larger values use the admin `uint64` tagged
representation described below.

The browser offers page sizes 25, 50, 100, and 200 rows and initially selects
50. The JSON endpoint accepts only limits from 1 through 200 and offsets from 0
through 1,000,000. Counts locate the requested shard-major slice; the page then
reads only those bounded slices and concatenates shards in ascending physical
order. Within each file, it prefers a proven primary-key order, preserving the
key index's collation and direction. An exact `INTEGER PRIMARY KEY` uses its
rowid-backed order without another index. A table without a safe primary-key
order uses an unshadowed intrinsic `rowid`, `_rowid_`, or `oid`; only the
rare table with no safe unique physical key falls back to every visible column.
The response's `ordering` field identifies the selected contract. Duplicate
rows remain representable, and the browser reads at most one extra logical row
to derive `has_more`. The combined page is checked against one configured row
and logical-byte budget. The extra row is not serialized, and `has_more`
remains false when the next offset would exceed 1,000,000.

Primary-key and rowid ordering avoid the temporary full-table sort previously
required by wide tables. Offset paging still scans up to the requested local
offset; a future cursor API can provide constant-work deep-page navigation.

Each count and page is a set of separate committed file reads, not an atomic
cross-file or retained multi-page snapshot. Concurrent inserts or deletes can
therefore move or repeat rows between requests. The browser accepts no caller
SQL or arbitrary filter, aggregation, ordering, or pagination expression.

## Engine and JSON boundaries

The HTTP adapter does not open shard files or call `rusqlite`. Discovery, count,
and row reads coordinate BriskDB's bounded explicit-shard inspection operation
according to catalog placement. Each inspection uses the ordinary engine
lifecycle, schema gate, per-shard pool and worker admission, cancellation,
deadline, and effective result limits. The merged page receives one additional
combined budget check. SQLite must classify every generated statement as
read-only before it is stepped.

Results retain ordered column metadata and positional row arrays. Duplicate or
empty column names remain representable. Nulls, finite floats, valid text, and
integers in JavaScript's inclusive exact range
`-9007199254740991..=9007199254740991` use direct JSON values. Larger signed or
unsigned integers use
`{"$briskdb_type":"int64","value":"exact decimal text"}` or the equivalent
`uint64` tag in this admin-only response so the browser never rounds the
displayed value. Blobs are
arrays of byte-valued integers; decimals use strings; invalid UTF-8 text is
rendered lossily; and non-finite floats become JSON `null`. This tagged integer
addition does not change the experimental `/v1/query` response.

Engine failures use the same fixed, redacted HTTP problem details as other HTTP
operations. Login or session rejection returns HTTP 401 without echoing the
submitted password or session token. A failed discovery, count, or page read
does not invalidate a valid browser session, and a later valid request can
continue.

## Deliberate non-goals

The browser does not add editing, arbitrary SQL, schema migration, backup,
maintenance, a separate admin listener, an atomic cross-file snapshot, stable
pagination across concurrent writes, general distributed SQL aggregation or
ordering, durable users or roles, credential configuration, or TLS. Those
remain separate roadmap work. The browser adds no manifest table, file,
version, checksum input, migration, or recovery step.
