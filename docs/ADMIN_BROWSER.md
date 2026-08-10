# Admin data browser

Status: implemented for roadmap issue #106 and hardened by issue #110

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
Anyone who can reach the HTTP listener knows these credentials. Keep the current
HTTP service on a trusted network and do not treat this login as a production
identity boundary. The existing `/health` and `/v1/*` endpoints retain their
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
| `GET /admin/api/overview?shard=N` | Return `shard_count`, `selected_shard`, and the selected shard's binary-ordered `tables`; omitted `shard` selects zero |
| `GET /admin/api/count?table=T` | Return the exact physical-row sum for `T` as `table`, `scope: "all_physical_shards"`, `shard_count`, and `total_rows` |
| `GET /admin/api/rows?shard=N&table=T&limit=L&offset=O` | Return one page as `shard`, `table`, `limit`, `offset`, `has_more`, ordered `columns`, and positional `rows`; limit defaults to 50 and offset to zero |

The shell and its three assets are public so the application can render its login
state. Session, overview, count, and row calls require the server-side session check;
logout remains an idempotent cookie-clearing operation. Hiding controls in
JavaScript is not the access check.

Display conversion and authentication-order transitions live in the small
`logic.js` asset. Executable Node.js tests cover that pure logic, while a Rust
integration test syntax-checks both scripts and runs the logic suite as part of
the existing all-target CI command. Node.js is needed only for development
tests; serving the embedded browser has no frontend build step.

## Physical-shard view

The explorer intentionally selects a physical shard number. This differs from
normal `/v1/query` routing, which hashes a caller-provided logical `shard_key`.
The overview accepts only `0 <= shard < shard_count`. It discovers ordinary
tables in SQLite's `main` schema and excludes SQLite-owned names beginning with
`sqlite_` plus the exact BriskDB-owned name `briskdb` and names beginning with
`briskdb_`, using ASCII-case-insensitive prefix checks. Views and internal tables
are not presented. Names have a stable binary ordering in the response.

Discovery describes the tables physically present on the selected shard. It is
not the advisory logical `briskdb_tables` catalog, and it makes no claim that a
table exists on another shard. Guessing an excluded or absent name does not make
it browseable.

The browser offers page sizes 25, 50, 100, and 200 rows and initially selects
50. The JSON endpoint accepts only limits from 1 through 200 and offsets from 0
through 1,000,000. BriskDB validates the shard, table identity, limit, offset,
and checked pagination arithmetic before running the row read. The continuation
indicator is derived by reading at most one row beyond the requested page; that
extra row is not included in the returned page. It remains false when the next
offset would exceed 1,000,000, even if more physical rows exist.

Each page is a separate read of the selected shard. Offset pagination is
therefore a live view, not a retained snapshot: inserts or deletes between page
requests can move or repeat rows, and SQLite's table scan supplies no general
ordering guarantee. The explorer does not merge row pages or implement the
later general scatter/gather roadmap.

## All-shard physical row total

Selecting a table also starts a separate exact count request. BriskDB verifies
that the same exact ordinary table is browseable on every configured shard,
runs a generated read-only `COUNT(*)` through the engine on each shard with at
most eight shard operations in flight, and checked-adds the results as an
unsigned 64-bit total. One missing table, shard failure, deadline, completed
schema migration, or arithmetic overflow fails the whole request; no partial
total is returned. The count is loaded once per table selection rather than on
every pagination request.

`scope: "all_physical_shards"` is literal. A properly partitioned table's sum
is its logical row count. A replicated or global table counts every physical
copy, so identical rows stored on two shards contribute twice. The physical
browser cannot safely infer deduplication from the advisory catalog, especially
for imported uncataloged tables. Each shard count is also a separate live read,
not one atomic cross-shard snapshot, so concurrent writes can affect which
instant each shard represents.

`total_rows` uses a direct JSON integer through `9007199254740991`; larger
values use the admin `uint64` tagged representation described below. The page
summary continues to state the selected physical shard so the global count is
not mistaken for globally merged displayed rows.

## Engine and JSON boundaries

The HTTP adapter does not open shard files or call `rusqlite`. Discovery, count,
and row reads use BriskDB's bounded explicit-shard inspection operation. That operation
uses the ordinary engine lifecycle, schema gate, per-shard pool and worker
admission, cancellation, deadline, and effective result limits. SQLite must
classify its generated statement as read-only before it is stepped.

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

Issue #106 does not add editing, arbitrary SQL, schema migration, backup,
maintenance, query cancellation, a separate admin listener, stable pagination
across concurrent writes, general scatter/gather browsing, durable users or roles,
credential configuration, or TLS. Those remain separate roadmap work. The
browser adds no manifest table, file, version, checksum input, migration, or
recovery step.
