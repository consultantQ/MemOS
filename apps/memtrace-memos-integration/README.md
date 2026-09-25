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
find .memos/memtrace -type f -name '*.json' -print
```

Files are stored under a user-specific directory, one JSON graph per MemOS `trace_id`. Open a file to inspect `data.nodes`, `data.edges`, `data.operations`, and `data.sessions`. User and trace path components contain a readable prefix plus a hash, so the filename is not necessarily the raw trace ID. The graph's `graph_id` contains the trace ID.

## Configuration

Set these environment variables before loading the plugin. Restart MemOS after changing them.

| Variable | Default | Purpose |
| --- | --- | --- |
| `MEMOS_ENABLED_PLUGINS` | unset | Include `smartcomment` to enable this opt-in plugin. |
| `MEMOS_SMARTCOMMENT_OUTPUT_DIR` | `.memos/memtrace` | Directory for JSON graphs. |
| `MEMOS_SMARTCOMMENT_QUEUE_SIZE` | `1024` | Maximum queued trace events; a full queue drops events with a warning. |
| `MEMOS_SMARTCOMMENT_MAX_VALUE_CHARS` | `20000` | Maximum length of each captured string. |
| `MEMOS_SMARTCOMMENT_STRICT` | `false` | Enable smartcomment identity checks. |
| `MEMOS_SMARTCOMMENT_PROJECT_ID` | `memos` | Project identifier stored in graphs. |
| `MEMOS_SMARTCOMMENT_MAX_CACHED_TRACES` | `64` | Maximum graphs retained in memory. |
| `MEMOS_SMARTCOMMENT_MAX_CACHED_GRAPH_ITEMS` | `50000` | Maximum cached nodes, edges, operations, and sessions. |
| `MEMOS_SMARTCOMMENT_CACHE_TTL_SECONDS` | `300` | Idle cache expiration in seconds. |

Snapshots are written atomically. An idle or full in-memory cache can be evicted; a later event restores the saved graph and appends to it. Cache limits do not delete JSON files. Use a different output directory for each writer process, including when running multiple server workers.

Credential-named fields are redacted, embedding/vector fields are omitted, and long values are bounded before events enter the queue. JSON strings in `argument` and `arguments` are decoded and recursively redacted; unparseable argument strings are omitted. **Free-form message and memory text can still appear in graphs**, up to the length limit. Store the output directory with access appropriate for that data.

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

Each handler lists Hook callbacks before its internal helpers. `AddHandler` and
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
- `persisted_memory`: the memory after `text_memory.add.after` confirms the write.

The asynchronous MemRead scheduler also emits:

- `enhanced_memory`: a fine-transfer result reused as the input to the enhanced-memory
  database write;
- `scheduler_result`: a string result for operations such as archive, delete, refresh,
  or failure that do not otherwise expose a graphable output value.

Scheduler fine-transfer inputs reuse the `memory:<cube_id>:<memory_id>` identity created
by the earlier TextMemory persistence event. This connects the original persisted memory
to its enhanced successor without creating a duplicate memory node. Archive, delete,
refresh, and failed MemRead operations are also recorded; failures include only the error
type, not the error message or an error-value node.

A persisted memory unit is one immutable graph anchor per cube and memory ID. Its first
complete snapshot is retained; different later observations log a warning without replacing
it or creating a new version, including in strict mode. If an asynchronous reference arrives
before the write event, its ID-only placeholder is completed once without changing the node
ID or existing edges. Restoring older snapshots also joins split versions of persisted units
to their earliest anchor and retains the first complete snapshot and all incident edges.

Fine-transfer Hooks declare `operation_input.result_grouping`: `per_input` means
one group per input in input order, including empty groups for failures; `batch`
means the whole input batch contributes to its outputs. The MemRead scheduler Hook
identifies SimpleStructMemReader's transfer implementation (including inherited uses
such as StrategyStructMemReader) as `per_input`, and MultiModalStructMemReader's as
`batch`. A Reader's explicit `fine_transfer_result_grouping` declaration takes
precedence; custom transfer implementations otherwise remain `unknown`. If grouping is
unknown or inconsistent for multiple inputs, the plugin preserves the nodes and
marks lineage as unresolved instead of guessing source edges. Failed enhanced
writes retain their enhanced-memory inputs and links to the failure status.

The integration follows the current MemOS operation-boundary Hook contract: callbacks
receive correlation-only `HookContext` data plus explicit business keyword arguments.
Successful MemReader, TextMemory, and scheduler callbacks observe the piped `result`;
their matching `failed` callbacks record a status node without changing the propagated
exception. MemReader and TextMemory failures also omit exception messages.

Database write exceptions propagate through `MemoryManager.add`, including batch and
parallel single-node writes. A failed add emits the existing failure Hook instead of a
successful persistence event, and a failed enhanced write skips source-memory deletion
for that MemRead operation. Some concurrent writes may already have committed; failure
describes the operation outcome and does not imply rollback of those writes.

The Search API emits this ordered result pipeline:

```text
search_query
  -- memos.search.retrieve --> raw memory nodes
  -- memos.search.threshold_filter --> threshold memory nodes; removed nodes -> "filtered"
  -- memos.search.deduplicate --> deduplicated memory nodes; removed nodes -> "filtered"
  -- memos.search.rerank --> reranked memory nodes
```

Every Search stage materializes its current candidates as separate, stage-scoped
`search_memory` nodes. The node value contains the memory ID, text, score, result type, and
cube ID. Its comment records the one-based `rank`, stage, rank scope, and global position.
Rank scope is one result-type/cube bucket because the Search API returns bucketed results.
The current API rerank stage is recorded as a descending `metadata.relativity` sort
(`relativity_sort`). Search nodes describe the Search pipeline and do not reference
persisted memory units.

Search stages use explicit links rather than the input/output Cartesian product. Threshold
and deduplication link each surviving source to its new stage node, while removed memories
link to one stage-scoped `"filtered"` string node. Rerank likewise links each
source to the new node with the same memory ID, including knowledge memories whose text is
expanded with original document content. Matching uses result type, cube and memory ID;
candidates without an ID use a content fingerprint computed before truncation. New
candidates are retained even when no source edge is known. This keeps every Hook visible as
one step in the Search flow, including stages whose values did not change.

Search observation ends at `search.results.after_rerank`, which releases the operation's
cached stage nodes. All Search hooks use the shared `HookContext.operation_id` as their graph and cache
scope, so multiple searches in one trace cannot reuse or clear each other's stage nodes.
Post-processing failures clear only the failed operation's cached stage nodes.

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

Callers that previously relied on omitted `links` or `links=None` for automatic
input/output edges must now provide explicit links. Match endpoints using each
operation's data contract; unknown lineage keeps its nodes without a guessed edge.

## Run tests

From the MemOS repository root, in the same Python environment:

```bash
python -m pytest apps/memtrace-memos-integration/tests -q
```
