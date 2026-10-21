# MemOS smartcomment integration

Trace how memories move through MemOS add, Search API, and asynchronous MemRead operations. This external plugin listens to existing MemOS Hooks and writes a smartcomment execution graph as JSON for each trace. Installing the plugin does not enable it automatically.

**Dependency:** use the [public `smartcomment` package on PyPI](https://pypi.org/project/smartcomment/). The plugin requires `smartcomment>=0.1.2`; you do not need a smartcomment source checkout. The integration in this directory is installed separately.

## Quick start

### 1. Install into the MemOS environment

Use Python 3.12 or newer. Activate the **same environment that runs MemOS**, then run these commands from the MemOS repository root:

```bash
# Skip this line if MemOS is already installed in the active environment.
python -m pip install -e .
python -m pip install "smartcomment>=0.1.2"
python -m pip install -e apps/memtrace-memos-integration
```

The integration package also declares both MemoryOS and smartcomment as dependencies, but installing the public smartcomment package explicitly makes its source clear. If your MemOS environment was created with `uv` and has no `pip` module, use its interpreter explicitly instead:

```bash
uv pip install --python .venv/bin/python -e .
uv pip install --python .venv/bin/python "smartcomment>=0.1.2"
uv pip install --python .venv/bin/python -e apps/memtrace-memos-integration
```

Skip the first line if MemOS is already installed there.

Check that both packages and the MemOS plugin entry point are visible to this interpreter:

```bash
python -c 'from importlib.metadata import version, entry_points; print("smartcomment", version("smartcomment")); print("integration", version("memos-smartcomment-integration")); print("plugin", [ep.name for ep in entry_points(group="memos.plugins") if ep.name == "smartcomment"])'
```

The last line should show `plugin ['smartcomment']`. If it does not, install the integration again with the interpreter used to start MemOS.

### 2. Enable the plugin and start MemOS

Configure the MemOS service as described in the [self-hosting guide](../../README.md). Set the plugin variables **before** starting the service:

```bash
export MEMOS_ENABLED_PLUGINS=smartcomment
export MEMOS_SMARTCOMMENT_OUTPUT_DIR=.memos/memtrace
python -m uvicorn memos.api.server_api:app --host 127.0.0.1 --port 8000
```

`MEMOS_ENABLED_PLUGINS` accepts comma-separated names if you use other opt-in plugins. The output path is relative to the service's working directory; use an absolute path when that directory may change. Startup should log `smartcomment plugin loaded`.

### 3. Run a memory operation and inspect the graph

Call a working MemOS `/product/add` or `/product/search` endpoint (available in the service's `/docs` page). Add records the message → extraction → persistence flow; Search records retrieval, filtering, deduplication, and reranking. MemRead scheduler events may arrive later and extend the same trace.

After the operation, list the saved graphs:

```bash
find .memos/memtrace -type f -name 'index.md' -print
```

Open your user's `index.md` to browse requests, newest first. Each row includes the
request time with UTC offset, entry type, a bounded query/message preview, the full
trace ID, and a relative link to its `graph.json`.

```text
.memos/memtrace/
└── alice--<short-user-hash>/
    ├── _user.json
    ├── index.md
    ├── _index.json
    └── 2026-10-15/
        ├── search/
        │   └── 14-32-10.125__<trace-prefix>--<short-trace-hash>/
        │       ├── graph.json
        │       └── meta.json
        └── add/
            └── 14-35-22.410__<trace-prefix>--<short-trace-hash>/
                ├── graph.json
                └── meta.json
```

The date, time, and entry-type folders are fixed when a trace is first recorded.
`search` covers Search events; `add` covers Add, MemReader, and text-memory events.
Traces first observed through scheduler events use `memread`; other events use
`other`. Later asynchronous events, cache eviction, restarts, and timezone changes
continue the original graph without moving it or splitting it across folders.

Dates and times default to the **local timezone of the machine/container running
the MemOS service**, including daylight-saving rules at the event's date. No extra
configuration is needed. Set `MEMOS_SMARTCOMMENT_OUTPUT_TIMEZONE=local` explicitly
to restore this behavior if a timezone override was previously configured.
To force a specific timezone instead, set it before starting the service:

```bash
export MEMOS_SMARTCOMMENT_OUTPUT_TIMEZONE=Asia/Shanghai
```

`meta.json` records the full user/trace/project IDs, first/latest event timestamps,
timezone, entry type, summary, session/task IDs, cubes, operations, and relative
graph path. `_index.json` maps full trace IDs to that metadata. The writer rebuilds
its in-memory catalog from per-trace metadata on first access, and republishes the
two user indexes on the next successful write, recovering missing, corrupt, or
stale indexes. `_user.json` retains the full user identity. Readable ID prefixes
keep Unicode letters, are limited to 32 characters, and carry a 12-character hash;
hash collisions extend the suffix instead of overwriting another request.

Graph contents remain `data.nodes`, `data.edges`, `data.operations`, and
`data.sessions`, with `graph_id` equal to the complete trace ID. There is still one
graph per `(user_id, trace_id)`: reusing a trace across requests keeps them together.
For one graph per request, supply a unique trace ID at the request entry and carry
it through asynchronous tasks. This plugin does not change request trace IDs.
`adapter.output_path(trace_id, user_id)` resolves an allocated graph and raises
`FileNotFoundError` before the writer has allocated that trace.

**The previous flat `<user>/<trace>.json` layout is not read or migrated.** Existing
files are left untouched; graph restoration and Search lineage use only the new
layout. To list actual graphs, use `find .memos/memtrace -name graph.json` rather
than treating metadata/index JSON files as graphs.

## Configuration

Set these environment variables before loading the plugin. Restart MemOS after changing them.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MEMOS_ENABLED_PLUGINS` | unset | Include `smartcomment` to enable this opt-in plugin. |
| `MEMOS_SMARTCOMMENT_OUTPUT_DIR` | `.memos/memtrace` | Directory for JSON graphs. |
| `MEMOS_SMARTCOMMENT_OUTPUT_TIMEZONE` | `local` | Service machine's local timezone by default; an IANA name such as `Asia/Shanghai` or `UTC` overrides it. |
| `MEMOS_SMARTCOMMENT_QUEUE_SIZE` | `1024` | Maximum queued trace events; a full queue drops events with a warning. |
| `MEMOS_SMARTCOMMENT_MAX_VALUE_CHARS` | `20000` | Maximum length of each captured string. |
| `MEMOS_SMARTCOMMENT_STRICT` | `false` | Enable smartcomment identity checks. |
| `MEMOS_SMARTCOMMENT_PROJECT_ID` | `memos` | Project identifier stored in graphs. |
| `MEMOS_SMARTCOMMENT_MAX_CACHED_TRACES` | `64` | Maximum graphs retained in memory. |
| `MEMOS_SMARTCOMMENT_MAX_CACHED_GRAPH_ITEMS` | `50000` | Maximum cached nodes, edges, operations, and sessions. |
| `MEMOS_SMARTCOMMENT_CACHE_TTL_SECONDS` | `300` | Idle cache expiration in seconds. |
| `MEMOS_SMARTCOMMENT_LINK_SEARCH_TO_ADD` | `false` | Include the Add ancestry of final Search hits and link final result lists to persisted memory nodes. |

Snapshots are written atomically. An idle or full in-memory cache can be evicted; a later event restores the saved graph and appends to it. Cache limits do not delete JSON files. Use a different output directory for each writer process, including when running multiple server workers.

### Connect Search results to their Add history

```bash
export MEMOS_SMARTCOMMENT_LINK_SEARCH_TO_ADD=true
```

Restart MemOS after setting the switch. At `search.results.after_rerank`, the writer
matches each final result by `(cube_id, memory_id)` within the same user and project,
reads the existing JSON snapshots, and imports only the matched persisted node and
its upstream Add/MemRead dependencies into the Search graph:

```text
message batch -> extracted memory -> persisted memory -> enhanced memory
                                                            |
                                                            v
                                                    persisted memory
                                                            ^
                                             memory_search_lineage (rank, score)
                                                            |
                                                 final search memory list
```

The new edge points **from the final Search list to the persisted memory**. Its comment
shows `rank` (1-based position within that final list) and the observed `score`, for
example `rank=2, score=0.8`. Both fields are also stored in edge metadata. Zero scores
are preserved; missing scores are shown as `unknown` and stored as `null`. Only
explicit incoming dependencies are followed; sibling outputs from the same Add,
unrelated memories, downstream archive/delete branches, and prior Search graphs
are not imported. A shared ancestor is included once. If multiple source memories
actually contributed to a merged memory, all those ancestors remain relevant.
Message batches remain the original captured units; the plugin does not infer
which individual messages within a batch contributed to a memory.

Only final results trigger Add imports. Repeated hits in one list share a single link
using their first occurrence's rank and score; each final list links to the same memory
independently. Unresolved or anonymous entries still occupy their original list positions.
Missing IDs, unavailable Add history, and ambiguous identities produce no guessed
links. `memos.search.link_add_lineage` reports linked/unresolved counts for distinct
identifiable hits per list. Imported items retain `lineage_source_trace_id` metadata;
the original Add files are unchanged.

History lookup runs on the asynchronous writer and reads this user's indexed `graph.json` files across all dates and entry types
once per final Search event. Its temporary index grows with that user's captured
history; keep the output directory scoped to the history you need. It works after
cache eviction or restart and can use snapshots captured before enabling the switch.
Only history already written to this output directory is available at Search time;
late Add events do not retroactively update completed Search graphs. Existing nodes
already native to the same trace are retained. Turning the switch off affects future
Search events and does not remove links from previously saved graphs.

Credential-named fields are redacted, embedding/vector fields are omitted, and long values are bounded before events enter the queue. JSON strings in `argument` and `arguments` are decoded and recursively redacted; unparseable argument strings are omitted. **Free-form message and memory text can still appear in graphs**, up to the length limit. Request summaries copy at most 160 characters (plus an ellipsis) from the already-captured query or message batch into metadata and indexes; they do not call an LLM. Markdown display escapes user text. Store the output directory with access appropriate for that data.

## If no graph appears

1. Confirm the service uses the interpreter that passed the package and entry-point check above, and that `MEMOS_ENABLED_PLUGINS` included `smartcomment` before startup.
2. Check for `smartcomment plugin loaded` and any writer warnings in the service logs. A full queue drops events; failed writes are logged.
3. Confirm that an add or Search API operation ran successfully, then check the configured output directory relative to the service's working directory. Scheduler updates can arrive asynchronously.

## Development and graph semantics

The sections below describe the Hook mapping and explicit data-flow contract for maintainers. MemOS business modules do not import smartcomment. See also [`memos_smartcomment/CONVENTIONS.md`](memos_smartcomment/CONVENTIONS.md).

### Handler layout

Hook callbacks live in `memos_smartcomment/handlers/`:

- `add_handler.py`: `AddHandler` observes add, extraction, persistence, and subsequent
  MemRead scheduler operations; it owns pending message-batch correlation.
- `search_handler.py`: `SearchHandler` observes the Search API result pipeline and
  owns its operation-scoped stage cache.
- `base.py`: shared context parsing, snapshotting, event construction, and submission.
- `__init__.py`: `HookHandlers` combines both flows and clears both caches at shutdown.
  The existing `from memos_smartcomment.handlers import HookHandlers` import is unchanged.

Each handler lists Hook callbacks before its shared processing flow, followed by
correlation state, node construction, and lineage helpers. Short section comments mark
these responsibilities. `AddHandler` and
`SearchHandler` can also be instantiated independently with an event submission callable.

### Captured memory flow

```text
message batch
  -- mem_reader.extract --> extracted memory
  -- text_memory.persist --> persisted memory
  -- scheduler.mem_read.fine_transfer --> enhanced memory
  -- scheduler.mem_read.add_enhanced --> persisted memory
```

The synchronous add flow emits three semantic memory categories:

- `message_batch`: messages received at `add.before` and consumed by
  `mem_reader.extract.after`;
- `extracted_memory`: a MemReader output passed into the TextMemory writer;
- `persisted_memory`: the memory observed when `text_memory.add.after` returns its ID.

The asynchronous MemRead scheduler also emits:

- `enhanced_memory`: a fine-transfer result reused as the input to the enhanced-memory
  database write;
- `scheduler_result`: a string result for operations such as delete, soft-delete,
  or failure that do not otherwise expose a graphable output value.

Scheduler fine-transfer inputs reuse the `memory:<cube_id>:<memory_id>` identity created
by the earlier TextMemory persistence event. This connects the original persisted memory
to its enhanced successor without creating a duplicate memory node. Delete,
soft-delete, and failed MemRead operations are also recorded; failures include only the
error type, not the error message or an error-value node.

A persisted memory unit is one immutable graph anchor per cube and memory ID. Its first
complete snapshot is retained; different later observations log a warning without replacing
it or creating a new version, including in strict mode. If an asynchronous reference arrives
before the write event, its ID-only placeholder is completed once without changing the node
ID or existing edges. Current-format snapshots restore those anchors and edges directly,
including after cache eviction. Historical formats with split persisted-memory versions
or missing `snapshot_status` metadata are no longer migrated; use a separate output
directory instead of appending to those artifacts.

Fine-transfer Hooks declare `operation_input.result_grouping`: `per_input` means
one group per input in input order, including empty groups for failures; `batch`
means the whole input batch contributes to its outputs. The MemRead scheduler Hook
identifies SimpleStructMemReader's transfer implementation (including inherited uses
such as StrategyStructMemReader) as `per_input`, and MultiModalStructMemReader's as
`batch`. Grouping is inferred from the bound transfer implementation; custom overrides
remain `unknown`. If grouping is
unknown or inconsistent for multiple inputs, the plugin preserves the nodes and
marks lineage as unresolved instead of guessing source edges. Failed enhanced
writes retain their enhanced-memory inputs and links to the failure status.

The integration follows the current MemOS operation-boundary Hook contract: callbacks
receive correlation-only `HookContext` data plus explicit business keyword arguments.
Successful MemReader, TextMemory, and scheduler callbacks observe the piped `result`;
their matching `failed` callbacks record a status node without changing MemOS exception
handling. MemReader and TextMemory failures also omit exception messages.

Scheduler callbacks receive `hook_context`, `operation`, `operation_input`, and either
`result` or `error`. Scheduler message dictionaries are converted into `HookContext` by MemOS; the
plugin uses that context for trace, user, session, task, and cube correlation.

Fine-transfer `operation_input` includes `memories`, `type`, `custom_tags`, `user_name`,
`chat_history`, `user_context`, `is_upload_skill`, and `result_grouping`. Delete and
soft-delete inputs include `memory_ids` and `user_name`; soft-delete also includes
`preserved_memory_ids`. The plugin uses `user_name` only as a fallback cube identifier,
never as the user identity. Reader arguments do not become memory nodes, and preserved
IDs do not become deletion edges. See the [MemOS Hook reference](../../src/memos/plugins/HOOKS.md#scheduler-memory-operations)
for the upstream contract.

A successful Hook means the observed method returned; the plugin does not independently
verify database persistence. MemOS may catch and log lower-level write errors before
they reach the Hook boundary, so a `persisted_memory` node alone is not proof of a
successful database write. Failure Hooks only observe exceptions reaching their boundary.
If an enhanced-write exception reaches that boundary, subsequent source-memory deletion
is skipped for that operation. Failure does not imply rollback of writes already committed.

With all post-processing stages enabled, the Search graph has this ordered result
pipeline for each populated type/cube bucket:

```text
query -> memory type -> cube -> raw list -> threshold list -> dedup list -> rerank list
```

Raw retrieval creates `search_memory_type` and `search_cube` branch nodes followed
by one `search_memory_list` node per nonempty bucket. Empty types and cubes are
omitted. The three retrieval links use `memory_type_branch`, `memory_cube_branch`,
and `memory_retrieval`, respectively. Every enabled, observed stage creates one new
list node per observed bucket and connects the last recorded list directly to it.
A fully removed bucket continues as an empty list, preserving the stage chain.
The list categories are `search_memory_list_raw`, `search_memory_list_threshold`,
`search_memory_list_dedup`, and `search_memory_list_rerank`, so DOT colors each stage
separately. Their `class_name` remains `search_memory_list`.

The plugin follows the API's current stage conditions even though the API emits
Hooks for disabled stages: threshold is recorded only when `relativity > 0`, and
dedup only when `dedup` is `"sim"` or `"mmr"`. Disabled stages produce no events,
nodes, or edges. For example, `relativity=0` yields `raw -> dedup -> rerank` when
dedup is enabled; setting `dedup="no"` as well yields `raw -> rerank`. An enabled
stage is retained even if its results are unchanged. Missing intermediate Hooks
also leave the last recorded stage as the predecessor.

The API always runs its post-processing `rerank_knowledge_mem` on a successful
search, so its rerank stage is still recorded when `search_req.rerank=False`.
That request flag controls the internal retrieval reranker before raw, not this
post-processing stage. The plugin does not change which business stages execute.

List entries contain `memory_id`, `memory`, and `score`, in the order returned by
the Hook. Result type, cube ID, stage, bucket position, and `memory_count` belong to
the list node's metadata. Individual memories no longer create graph nodes, ranks,
or pairwise node matches. Snapshots are taken per entry so the outer list retains
all results, including lists longer than 200 entries; field truncation and redaction
still apply within each entry. With `MEMOS_SMARTCOMMENT_LINK_SEARCH_TO_ADD=true`,
final lists additionally link to matching persisted memory units, with per-list rank
and score on each edge.

Each stage records only its retained result lists. Removed entries do not create
separate list nodes or edges. `filtered_count` still counts removed occurrences across
buckets by memory ID (or original text for anonymous entries), including duplicates.
Reordering, expanded text, and score changes for the same ID do not count as removals.

Branches and lists use `trace_id` for stage connections. List links
use result type, cube ID, and the occurrence of that cube within the type; buckets
without IDs use their position. Reordered unique cube IDs remain correctly linked.
Repeated or missing cube IDs rely on stable occurrence order or position. A new
bucket without a previous-stage list is retained without a guessed source edge.

The plugin copies `HookContext.operation_id` unchanged into event, operation, and
edge metadata. Optional Add-history links retain the rerank Hook's operation ID.

Search observation ends at `search.results.after_rerank`. Each trace caches only
its latest recorded lists; skipped stages leave that cache intact. Rerank and
post-processing failure clear that trace's cache; shutdown clears all caches. The current rerank event describes
the API's descending `metadata.relativity` sort; the tracing handler preserves
observed order and does not sort results itself.

The exported graph omits smartcomment's internal `COMMENT:NONE@1` sentinel node, its
incident edges, and its sentinel session. Root inputs and outputs remain normal semantic
nodes; the internal placeholder does not participate in the MemOS graph.

The node `comment` explains its meaning and MemOS stage. Request wrappers, Hook boundary
objects, `info`, backend objects, errors, and scheduler variables are intentionally not
represented as graph nodes. Search queries and stage-scoped memory results are represented,
but raw backend objects are not. Scheduler operations
are not connected merely because one ran after another; the graph contains only explicit
data-flow edges. When an operation has no data output, its `scheduler_result` string is used
as the target of edges from the memory values it consumed.

### Trace field conventions

Keep machine identifiers separate from reader-facing descriptions:

- `identity` controls node reuse; SmartComment exports it as the node `name`.
  The unused internal `TraceValue.name` field has been removed.
- Node `category` names a business role; `class_name` is a type/identity namespace.
  Existing names are retained because consumers and node IDs can depend on them.
- `TraceEvent.operation` is the stable dotted operation name; its optional `comment`
  explains the observation in the exported operation and session. Each built-in link
  has its own description of the source-to-target relationship.
- Write comments describe returned IDs, not independently confirmed database writes.
  Search comments describe whole lists at each stage and distinguish removal
  from search results from deletion in storage. Scheduler comments identify the backend
  operation, including `delete` versus `soft_delete`, without claiming every input was removed.

These comments are displayed by graph viewers and MemTrace notebooks; they are not
parsing keys. The complete field tables and reading order are in
[`CONVENTIONS.md`](memos_smartcomment/CONVENTIONS.md).

### Correlation and snapshot isolation

Each add receives a fresh UUID for its message batch, including repeated adds that reuse
the same Python list. A bounded registry correlates the live request with MemReader
callbacks for each target cube. The registry uses weak references for API requests and
releases entries after extraction (including empty and failed results), `add.after`, or
shutdown. At most 1024 pending adds are retained; after eviction, an extraction receives
an independent input node instead of borrowing another request's identity.

Without a request trace context, stages use `standalone:<user_id>:<session_id>`, with
`default_session` matching MemOS's default. The task ID is a fallback only when the user
is unavailable, because MemReader does not receive the add request's task ID.
An explicit Hook context always takes precedence over a worker's ambient request context,
including when the Hook has no trace ID. This prevents one message from borrowing another
message's trace during scheduler batch processing. Hook callbacks capture detached snapshots
synchronously and enqueue them; the graph writer alone runs asynchronously.

Extracted and persisted memories use different identities so the database write remains
visible as a transition. Both synchronous and scheduler writes match returned IDs to
unique input memory IDs, allowing skipped inputs and reordered results. Unknown or
ambiguous IDs retain an ID-only persisted node without inferred content or source edges;
operation metadata marks lineage as `resolved`, `partial`, or `unresolved`.

### Event and link contract

All flows use the same edge path: Hooks build `TraceLink(source, target)`, and the
recorder writes each link with smartcomment's `comment_link` inside one
`comment_op_scope` per event. Link category and comment default to the operation's;
link metadata extends and can override the operation metadata on that edge.

`TraceEvent.links` defaults to `()`: omitted or empty links create no edges.
`inputs` and `outputs` register nodes without inferring dependencies. Link endpoints
are registered even when they are not separately listed as inputs or outputs.
Independent nodes and operations with no links are retained.

```python
event = TraceEvent(
    operation="memos.text_memory.persist",
    category="memory_persistence",
    trace_id=trace_id,
    inputs=(extracted,),
    outputs=(persisted,),
    links=(TraceLink(source=extracted, target=persisted),),
)
```

Always provide explicit links for known dependencies. Match endpoints using each
operation's data contract; unknown lineage keeps its nodes without a guessed edge.

## Verify the integration

Run the Search-to-Add lineage regression tests from this directory:

```bash
PYTHONPATH=. python -m pytest tests/test_search_lineage.py -q
```

They exercise actual Add/Search hooks, final-hit selection, shared ancestors,
cross-trace refinement, user/cube isolation, opt-in behavior, and durable restore.

Run the local search-branch regression tests (ignored by Git) from this directory
with the integration and its dependencies installed:

```bash
PYTHONPATH=. python -m pytest tests/handlers/test_search_handler.py -q
```

These tests cover populated and empty branches, list size independent of graph size,
filtered contents including duplicates, cube identity, operation isolation, and
exported list continuity through post-processing in strict and non-strict modes. Additional plugin regression tests are maintained locally.

For an installed integration, follow the Quick start package and entry-point check, enable
the plugin, and run a memory operation. Inspect the resulting JSON graph's `graph_id`,
`data.nodes`, and `data.edges`, and check the service logs for writer failures or dropped
events. Shutdown logs report accepted, processed, dropped, and failed event counts;
queue acceptance alone does not confirm persistence.
