# BriskDB launch kit

The campaign has one story:

> BriskDB turns ordinary SQLite files into one sharded database—with parallel
> writes, PostgreSQL compatibility, HTTP access, and embedded Rust/Python APIs.

Do not claim production readiness, multi-node high availability, complete SQL,
or a global-index speedup. Link to the alpha boundaries and ask for workloads,
client failures, and missing query shapes.

## Launch assets

- Repository: <https://github.com/schapman1974/briskdb>
- PyPI: <https://pypi.org/project/briskdb/>
- Alpha downloads: <https://github.com/schapman1974/briskdb/releases>
- Demo GIF: [`assets/briskdb-demo.gif`](assets/briskdb-demo.gif)
- Social preview: [`assets/social-preview.png`](assets/social-preview.png)
- Data browser: [`assets/admin-browser.svg`](assets/admin-browser.svg)
- Demo source: [`../examples/launch_demo.py`](../examples/launch_demo.py)
- Roadmap: [`../ROADMAP.md`](../ROADMAP.md)

Regenerate both image assets from the tested demo and the retained artwork:

```bash
uv run --with briskdb --with pillow examples/render_launch_assets.py
```

## Show HN

**Title**

```text
Show HN: BriskDB – sharded SQLite with a PostgreSQL wire protocol
```

**Post**

```text
Hi HN — I built BriskDB, an open-source Rust engine that turns ordinary SQLite
files into one sharded database.

The idea is simple: each shard keeps its own SQLite WAL and writer lock, so
writes routed to different shards can progress in parallel. A shared engine
adds virtual-bucket routing, shard-safe generated IDs, bounded scatter/gather,
HTTP, a PostgreSQL wire protocol, and embedded Rust/Python APIs. Every shard is
still a normal SQLite file; BriskDB does not fork SQLite.

What works now:
- compiler-free Python wheels: pip install briskdb
- macOS/Linux ARM64 and x86-64 binaries
- PostgreSQL clients including psql, psycopg, SQLAlchemy, and tokio-postgres
- HTTP API, browser data explorer, Prometheus global-index metrics
- same-host multi-process access to one ready data directory

It is an alpha. HTTP is loopback-only, authorization is incomplete, there are
no general cross-shard transactions, and global indexes currently prune shards
correctly but are slower on small hot data. Those results are published rather
than hidden.

The README has a 30-second wheel demo. I would especially value feedback on the
storage model, PostgreSQL client behavior, and which real workload should drive
the next compatibility work.

https://github.com/schapman1974/briskdb
```

## Reddit

### r/rust

**Title**

```text
BriskDB: one Rust engine for sharded SQLite, PostgreSQL, HTTP, and Python
```

**Body**

```text
I have been building BriskDB, an MIT-licensed Rust database engine that routes
one logical database over independent SQLite WAL files. The interesting Rust
boundary is that PostgreSQL, HTTP, the native service, PyO3, and embedding all
share the same typed sessions, limits, cancellation, errors, and query engine.

Different shard files provide different SQLite writer locks. Generated IDs use
non-overlapping native ranges or fenced hi/lo leases, and independently spawned
processes can share one ready local data root.

It is alpha software and the README is explicit about the missing pieces. I
would appreciate review of the concurrency model and public Rust API:
https://github.com/schapman1974/briskdb
```

### r/sqlite

**Title**

```text
Experiment: parallel SQLite write domains while keeping every shard a normal file
```

**Body**

```text
BriskDB does not change SQLite. It hashes virtual buckets onto multiple normal
SQLite WAL databases, so unrelated routed writes can use independent writer
locks. The manifest owns routing and IDs; application data stays in files that
sqlite3 and ordinary recovery tools can inspect.

The tradeoff is explicit shard semantics: no general cross-shard transaction,
limited global ordering/aggregation, and stopped-directory backup today. I have
documented the file format, recovery boundaries, and before/after measurements.

I would value feedback from people who know SQLite failure modes well:
https://github.com/schapman1974/briskdb
```

### r/databases

**Title**

```text
BriskDB alpha: sharded SQLite behind PostgreSQL and HTTP
```

**Body**

```text
BriskDB is a same-host sharded database built from ordinary SQLite files. It is
aimed at the space between one embedded SQLite file and a full distributed
PostgreSQL cluster: parallel writes across shard WALs, PostgreSQL/HTTP access,
and Rust/Python embedding from one engine.

This is not an HA or production claim. I published the missing transaction,
backup, SQL, authorization, and global-index performance boundaries in the
README. The current question is whether this middle ground is useful to real
applications.

Demo and architecture: https://github.com/schapman1974/briskdb
```

## Lobsters

**Title**

```text
BriskDB: sharded SQLite files behind one Rust query engine
```

**Text**

```text
Each shard is an ordinary SQLite WAL database with its own writer domain. A
protocol-neutral Rust engine owns routing, cancellation, limits, generated IDs,
global-index correctness, PostgreSQL, HTTP, and PyO3. The release gate includes
multi-process crash tests and publishes a negative performance result for the
current global-index metadata path. Feedback on the architecture is welcome.
```

## LinkedIn

```text
I have released a new BriskDB alpha.

BriskDB turns ordinary SQLite files into one sharded database: parallel writes
across independent WALs, PostgreSQL and HTTP access, plus embedded Rust and
Python APIs—all through the same engine.

The part I care about most is that the files remain SQLite. The sharding layer
adds protocols and coordination without replacing the storage engine developers
already trust.

It is still an honest alpha: no general cross-shard transactions, limited
global query shapes, and published performance holds where optimization is not
yet a win.

The README now has a 30-second demo with compiler-free Python wheels:
https://github.com/schapman1974/briskdb

#rust #sqlite #postgresql #opensource #databases
```

## X

**Launch post**

```text
BriskDB turns ordinary SQLite files into one sharded database.

• parallel writes across shard WALs
• PostgreSQL + HTTP
• embedded Rust + Python
• shard-safe IDs
• no SQLite fork

It is an honest open-source alpha. Try the 30-second demo:
https://github.com/schapman1974/briskdb
```

**Follow-up thread hooks**

```text
1/ Why multiple SQLite files? One WAL means one writer. Independent shard WALs
create independent writer domains while preserving SQLite's storage engine.

2/ Why PostgreSQL? Applications can test BriskDB with clients they already use
while the database semantics stay in one protocol-neutral Rust core.

3/ Why ordinary files? Debugging, inspection, backup, and recovery are much less
mysterious when the application rows remain in SQLite databases.

4/ What is missing? General cross-shard transactions, broad global SQL,
production auth/observability, online backup, and resharding. Alpha means alpha.
```

## Discord and Slack

Ask a moderator before posting. Use one short message, not the full launch copy:

```text
I built an open-source Rust experiment that shards one logical database across
ordinary SQLite WAL files, then exposes it through PostgreSQL, HTTP, Rust, and
Python. The README has a 30-second wheel demo and explicit alpha limitations.
Would architecture feedback be appropriate here? https://github.com/schapman1974/briskdb
```

## Four technical follow-ups

1. **How BriskDB gets parallel SQLite writes**
   - SQLite's one-writer-per-WAL rule.
   - Virtual buckets and stable shard ownership.
   - Contention tests: same shard versus different shards.
   - What this does not solve: atomic cross-shard writes.
2. **Shard-safe IDs without a central per-row bottleneck**
   - Native positive `AUTOINCREMENT` ranges.
   - Fenced hi/lo leases and crash gaps.
   - Multi-process collision tests and explicit overflow limits.
3. **Building a PostgreSQL wire protocol over SQLite**
   - Protocol translation versus pretending to be PostgreSQL internally.
   - Simple/extended queries, prepared parameters, cancellation, TLS/SCRAM.
   - Compatibility tests with real clients and the unsupported SQL boundary.
4. **Why every BriskDB shard remains a normal SQLite file**
   - Inspectability and recovery benefits.
   - Manifest responsibilities and file identity validation.
   - Backup limitations, WAL sidecars, and the no-network-filesystem rule.

## Four-week schedule

Use a Tuesday launch. Schedule the drafts, but keep final posting manual so a
human can check community rules, answer immediately, and stop if feedback shows
a serious bug.

| Day | Channel | Asset | Goal |
| --- | --- | --- | --- |
| Week 1 Tue, 9:00 ET | Hacker News | Show HN post + GIF | Main technical launch; remain available for replies |
| Week 1 Tue, 11:30 ET | X | Launch post | Point to the same story after HN has context |
| Week 1 Wed | r/rust | Rust-specific post | Engine/API review, not generic promotion |
| Week 1 Thu | Lobsters | Short technical submission | Architecture discussion |
| Week 1 Fri | LinkedIn | Launch narrative + social card | Reach professional database users |
| Week 2 Tue | Project blog or GitHub Discussion | Parallel-writes article | Explain the core differentiator |
| Week 2 Wed | r/sqlite | SQLite-specific post | Invite failure-mode scrutiny |
| Week 2 Fri | X/LinkedIn | One measured chart from the article | Sustain interest without repeating launch copy |
| Week 3 Tue | Project blog or GitHub Discussion | PostgreSQL protocol article | Attract client and protocol contributors |
| Week 3 Thu | r/databases | Database tradeoff post | Ask whether the middle ground is useful |
| Week 4 Tue | Project blog or GitHub Discussion | Ordinary-files article | Reinforce the inspectability story |
| Week 4 Thu | X/LinkedIn | MongoDB/MySQL roadmap preview | Give stars a concrete reason to follow |
| Week 4 Fri | GitHub Discussion | Transparent launch recap | Publish stars, installs, issues, failures, and next priorities |

For each scheduled item:

- answer every substantive reply during the first two hours;
- record stars, PyPI installs where available, release downloads, issues, and
  repeat visitors at 24 hours and seven days;
- do not repost unchanged copy into multiple communities;
- pause the campaign if installation or data-safety bugs appear; and
- turn recurring questions into README fixes or issues before the next post.

The first target is 100 genuine stars. More important signals are successful
installs, tested applications, useful issues, and contributors who return.
