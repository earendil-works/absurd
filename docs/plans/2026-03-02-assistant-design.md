# Absurd Assistant: AI Personal Assistant Design

A nanoclaw-inspired AI personal assistant built on Absurd as the durable task
management and message bus layer. Telegram is the primary communication channel.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Project location | Separate repository | Depends on Absurd TS SDK as a package |
| LLM backend | Claude API (Anthropic SDK) | Direct tool use, aligns with ecosystem |
| Architecture | Absurd-native | Every interaction is a durable task |
| Plugin isolation | Containerized (Docker/Podman) | Security via container sandboxing |
| Agent process | Runs on host | Agent needs broad access; only plugins are sandboxed |
| Agent notes | Markdown file on disk | Human-editable, LLM-native, git-trackable |
| Conversation history | Postgres table | Queryable, transactional |
| Plugin state | Postgres KV table | Simple get/set JSONB, promote to dedicated tables later |
| Scheduling | Absurd's built-in cron system | No external scheduler needed |
| Telegram library | grammY (long-polling) | No public endpoint required |
| Slash commands | None | All interaction through natural language via agent loop |
| Plugin hot-reload | File watcher on plugins/ directory | Reload tools without restarting worker |
| Multi-user | Single user only | Simplifies auth, memory, permissions |
| Self-update | Agent notes self-modification (core) + code self-update (stretch) | Absurd tasks survive worker restarts |

## System Architecture

```
+-----------------------------------------------------+
|                    Process                            |
|                                                       |
|  +--------------+    +--------------------------+    |
|  | Telegram Bot  |--->|  Absurd SDK (Worker)     |    |
|  | (grammy)      |    |  - handle-message task   |    |
|  | long-polling   |    |  - plugin:* tasks        |    |
|  +--------------+    |  - scheduled tasks        |    |
|                       +------------+-------------+    |
|                                    |                  |
|  +--------------+    +-------------v-----------+     |
|  | Plugin Loader |--->|  Plugin Runtime         |     |
|  | (hot-reload)  |    |  (container execution)  |     |
|  +--------------+    +-------------------------+     |
|                                                       |
+---------------------+-------------------------------+
                       |
              +--------v--------+
              |   PostgreSQL     |
              |  - Absurd queues |
              |  - Message log   |
              |  - Plugin state  |
              +-----------------+
```

Single Absurd queue (`assistant`) for all tasks. Per-task retry strategies and
cancellation policies handle differentiation.

## Message Flow

1. User sends a Telegram message
2. grammY handler spawns `handle-message` task with message payload
3. Absurd worker claims the task
4. **Step "load-context"**: Read `data/notes.md`, query last N messages from
   Postgres, gather active plugin tool definitions
5. **Step "agent-loop-{i}"** (checkpointed per iteration):
   - Call Claude API with system prompt + history + tools
   - If Claude returns text: send to Telegram, done
   - If Claude returns tool_use: execute plugin in container
   - Append tool result, loop back to Claude
6. **Step "persist"**: Save outbound message to history, update agent notes if
   the agent learned something

Each Claude API call is checkpointed. If the worker crashes mid-conversation,
it resumes from the last completed LLM call (no duplicate API spend).

## Plugin System

### Plugin Structure

```
plugins/
  reminder/
    plugin.json        # manifest: name, tools, permissions, schedule
    Dockerfile         # container definition
    index.ts           # entry point
```

### Plugin Manifest

```json
{
  "name": "reminder",
  "version": "1.0.0",
  "description": "Set and manage reminders",
  "tools": [
    {
      "name": "set_reminder",
      "description": "Set a reminder for a specific time",
      "input_schema": { "type": "object", "properties": { ... } }
    }
  ],
  "permissions": {
    "network": false,
    "volumes": [],
    "env": []
  },
  "schedules": [
    {
      "name": "check-reminders",
      "cron": "* * * * *",
      "task": "reminder:check-due"
    }
  ]
}
```

### Plugin Execution

When Claude decides to use a tool:

1. Agent loop spawns a short-lived container from the plugin's Docker image
2. Tool input is passed as JSON via stdin
3. Container executes, writes result to stdout
4. Container exits, result is captured and returned to Claude

### Plugin Data Access

Plugins communicate with the core via a stdin/stdout JSON protocol:

```json
{"action": "storage.get", "key": "active_reminders"}
{"action": "storage.set", "key": "active_reminders", "value": [...]}
```

All plugin state is stored in Postgres via the generic KV table. If a plugin
needs more complex queries in the future, it can be promoted to a dedicated
schema with migrations.

### Plugin Schedules

Plugins declare recurring tasks in their manifest. On load, the system calls
`absurd.createSchedule()` for each entry. Absurd's `tick_schedules()` fires
automatically on every `claim_task()` -- no external cron daemon needed.

### Hot Reload

A file watcher (chokidar) monitors the `plugins/` directory:

- **New plugin**: load manifest, register tools, build Docker image
- **Modified plugin**: rebuild image, update tool definitions
- **Removed plugin**: unregister tools, delete schedules

Tool definitions are refreshed on the next `handle-message` task without
restarting the worker.

## Memory & State

### Agent Notes (`data/notes.md`)

A single markdown file the agent reads on every message and updates when it
learns something meaningful. Human-editable and git-trackable.

```markdown
# User Profile
- Name: ...
- Timezone: ...
- Communication style: ...

# Learned Preferences
- ...

# Ongoing Context
- ...

# Working Memory
- ...
```

**Why a file, not Postgres?** Agent notes are read in full every time, never
queried or filtered. A file read is simpler than a DB query for this access
pattern. The user can edit notes with any text editor.

### Conversation History (Postgres)

```sql
CREATE SCHEMA assistant;

CREATE TABLE assistant.messages (
  id          BIGSERIAL PRIMARY KEY,
  chat_id     BIGINT NOT NULL,
  role        TEXT NOT NULL,        -- 'user', 'assistant', 'system'
  content     TEXT NOT NULL,
  tool_use    JSONB,
  created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_messages_chat
  ON assistant.messages(chat_id, created_at DESC);
```

### Plugin State (Postgres KV)

```sql
CREATE TABLE assistant.plugin_state (
  plugin_name TEXT NOT NULL,
  key         TEXT NOT NULL,
  value       JSONB NOT NULL,
  updated_at  TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (plugin_name, key)
);
```

### Context Window Strategy

When building the Claude prompt:

1. Always include: system instructions + agent notes + tool definitions
2. Fill remaining context with recent messages (newest first)
3. If history exceeds budget, summarize older messages into a "Previously..."
   block (itself a checkpointed step)

## Scheduling & Recurring Tasks

All scheduling uses Absurd's built-in cron system:

```typescript
// Plugin-declared schedule (from plugin.json, registered on load)
await absurd.createSchedule("reminder:check-due", "reminder:check", "* * * * *");

// User-created via natural language
// "Remind me every morning at 9am to check email"
await absurd.createSchedule(
  "user:morning-email",
  "send-reminder",
  "0 9 * * *",
  { params: { chatId, text: "Time to check your email!" } }
);
```

`tick_schedules()` fires on every `claim_task()` call. No external scheduler.

## Self-Update

### Level 1: Agent Notes Self-Modification (Core)

The agent has tools to read and update its own notes file. After conversations
where it learns something important, it appends to `data/notes.md`. A periodic
consolidation step (agent rewrites notes to be more concise) prevents unbounded
growth.

### Level 2: Code Self-Update (Stretch Goal)

A `self-update` plugin that:

1. Runs on a schedule (e.g., daily)
2. Pulls latest code from git
3. Compares against running version
4. If changes detected, gracefully shuts down the worker (`worker.close()`)
5. A process supervisor (systemd, Docker restart policy) restarts with new code
6. Absurd's durability ensures no in-flight tasks are lost -- they are reclaimed
   after restart

## System Prompt Structure

```
[Base personality and instructions]
[Contents of data/notes.md]
[Active plugin tool descriptions]
[Current date/time, user timezone]
---
[Dynamically queried plugin summaries (upcoming reminders, active todos)]
---
[Last N messages from conversation history]
```

## Agent Introspection Tools

Instead of slash commands, the agent has built-in tools for introspection:

| Tool | Purpose |
|------|---------|
| `list_plugins` | Show loaded plugins and their tools |
| `list_schedules` | Show active Absurd schedules |
| `get_status` | Worker status, uptime, task counts |
| `reload_plugins` | Trigger plugin hot-reload |
| `read_notes` | Read current agent notes |
| `update_notes` | Update agent notes file |

The user interacts with these through natural language ("what plugins do you
have?" or "reload your plugins").

## Project Structure

```
absurd-assistant/
  src/
    index.ts              # Entry: init Absurd, bot, worker
    bot.ts                # grammY bot setup
    tasks/
      handle-message.ts   # Main agent loop task
      send-message.ts     # Proactive outbound message task
    agent/
      loop.ts             # Claude API agent loop logic
      prompt.ts           # System prompt assembly
      tools.ts            # Tool registry (from loaded plugins)
    memory/
      history.ts          # Postgres message history CRUD
      notes.ts            # Agent notes file read/write
      plugin-state.ts     # Plugin KV store
    plugins/
      loader.ts           # Plugin discovery, hot-reload, image build
      runner.ts           # Container execution (docker/podman)
      protocol.ts         # stdin/stdout JSON protocol
    config.ts             # Config from env vars
  plugins/                # Plugin directories
    reminder/
      plugin.json
      Dockerfile
      index.ts
    web-search/
      plugin.json
      Dockerfile
      index.ts
    shell/
      plugin.json
      Dockerfile
      index.ts
  data/
    notes.md              # Agent notes (git-tracked)
  sql/
    assistant.sql         # Schema for messages + plugin_state
  package.json
  tsconfig.json
  Dockerfile              # For deploying the assistant itself
```

### Dependencies

- `absurd-sdk` -- Absurd TypeScript SDK
- `grammy` -- Telegram Bot API
- `@anthropic-ai/sdk` -- Claude API
- `dockerode` -- Docker API for container management
- `chokidar` -- File watching for plugin hot-reload

## Open Questions

- **Plugin marketplace/registry**: Should plugins be installable from a URL/git
  repo, or only local directories?
- **Multi-channel**: When adding channels beyond Telegram, should they be
  separate bot processes or a unified adapter layer?
- **Context window management**: What's the right threshold for triggering
  conversation summarization? Token count vs message count?
- **Agent notes consolidation**: Should this be automatic (periodic) or
  user-initiated?
