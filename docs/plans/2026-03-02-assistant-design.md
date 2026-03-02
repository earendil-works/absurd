# Absurd Assistant: AI Personal Assistant Design

A nanoclaw-inspired AI personal assistant built on Absurd as the durable task
management and message bus layer. Telegram is the primary communication channel.
The assistant runs entirely inside a Docker container for security, and can
extend its own capabilities by writing skill files and creating Absurd schedules.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Project location | Separate repository | Depends on Absurd TS SDK as a package |
| LLM backend | Claude API (Anthropic SDK) | Direct tool use, aligns with ecosystem |
| Architecture | Absurd-native | Every interaction is a durable task |
| Deployment | Entire agent in Docker | OS-level containment; no nested containers |
| Extensibility | Skills (markdown) + tool scripts | Agent can self-extend without code deployments |
| Agent notes | Markdown file on disk | Human-editable, LLM-native, git-trackable |
| Conversation history | Postgres table | Queryable, transactional |
| State storage | Postgres KV table | Simple get/set JSONB for skills and tools |
| Scheduling | Absurd's built-in cron system | No external scheduler needed |
| Telegram library | grammY (long-polling) | No public endpoint required |
| Interaction model | All natural language | No slash commands; agent handles everything |
| Multi-user | Single user only | Simplifies auth, memory, permissions |
| Self-update | Agent writes skills, tool scripts, and notes | Capabilities compound over time |

## System Architecture

```
Host machine
  |
  +-- Docker container (absurd-assistant)
  |     |
  |     +-- Node.js process
  |     |     +-- Telegram bot (grammY, long-polling)
  |     |     +-- Absurd worker (claims and executes tasks)
  |     |     +-- Agent loop (Claude API + tool dispatch)
  |     |
  |     +-- data/notes.md          (writable volume)
  |     +-- skills/*.md            (writable volume)
  |     +-- tools/*                (writable volume)
  |     +-- Network: Telegram API, Claude API, Postgres
  |
  +-- PostgreSQL
        +-- absurd schema (queues, tasks, runs, checkpoints, schedules)
        +-- assistant schema (messages, state)
```

Single Absurd queue (`assistant`) for all tasks. The container provides
OS-level security -- the agent can modify its own skills and tools but
cannot affect the host.

## Core Concepts

### Skills (Behavioral Instructions)

Skills are markdown files that describe recurring or on-demand behaviors.
The agent can read, create, modify, and delete them. Each skill has YAML
frontmatter and markdown instructions.

```markdown
---
name: daily-todo-summary
description: Send a daily summary of active TODOs every morning
schedule: "0 9 * * *"
---

# Daily TODO Summary

When this skill's schedule fires:

1. Read the TODO list from state (key: "todos")
2. Format as a numbered list with priorities
3. Send the summary to the user via Telegram
4. Call out any overdue items
```

Skills with a `schedule` field automatically get an Absurd schedule that
spawns an `execute-skill` task. Skills without a schedule are invoked
on-demand during conversations when the agent determines they are relevant.

### Tool Scripts (Executable Capabilities)

Tool scripts are executable files (shell, Python, etc.) that the agent
can create and invoke. They run inside the container with a timeout.

```bash
#!/bin/bash
# tools/github-prs.sh
# Input: JSON on stdin with { owner, repo }
input=$(cat)
owner=$(echo "$input" | jq -r '.owner')
repo=$(echo "$input" | jq -r '.repo')
gh pr list --repo "$owner/$repo" --json title,url,author --limit 10
```

Tool scripts receive JSON on stdin and write results to stdout. They
execute with `timeout 30s` inside the container.

### Absurd Tasks (The Execution Engine)

Every behavior is an Absurd task:

| Task | Purpose |
|------|---------|
| `handle-message` | Process a Telegram message through the agent loop |
| `execute-skill` | Run a skill's instructions via a mini agent loop |
| `send-message` | Send a proactive Telegram message |

The agent can spawn any of these via scheduling. A daily TODO summary
is just a scheduled `execute-skill` task. A reminder is just a scheduled
`send-message` task.

## Message Flow

1. User sends a Telegram message
2. grammY handler spawns `handle-message` task with message payload
3. Absurd worker claims the task
4. **Step "load-context"**: Read `data/notes.md`, query last N messages,
   discover available skills and tools
5. **Step "agent-turn-{i}"** (checkpointed per iteration):
   - Call Claude API with system prompt + history + tools
   - If Claude returns text: send to Telegram, done
   - If Claude returns tool_use: execute the tool
   - Append tool result, loop back to Claude
6. **Step "persist"**: Save messages to history

Each Claude API call is checkpointed. If the worker crashes mid-conversation,
it resumes from the last completed LLM call (no duplicate API spend).

## Scheduled Skill Execution

When a skill has a `schedule` field:

1. On startup (and when skills change), the system creates an Absurd schedule:
   `absurd.createSchedule("skill:daily-todo-summary", "execute-skill", "0 9 * * *")`
2. Absurd's `tick_schedules()` fires on every `claim_task()`
3. When due, it spawns an `execute-skill` task with `{ skillName, chatId }`
4. The task reads the skill file, runs a mini agent loop with those instructions
5. The agent loop can use any tools (send messages, run scripts, read state)

No external cron daemon. No polling loops. Just Absurd doing what it does.

## Agent Tools

### Built-in Tools (always available)

| Tool | Purpose |
|------|---------|
| `read_notes` | Read the agent notes file |
| `update_notes` | Update the agent notes file |
| `list_skills` | List all available skills |
| `read_skill` | Read a skill's content |
| `create_skill` | Write a new skill file (optionally with schedule) |
| `update_skill` | Modify an existing skill |
| `delete_skill` | Remove a skill and its schedule |
| `list_schedules` | List active Absurd schedules |
| `create_schedule` | Create an Absurd schedule directly |
| `delete_schedule` | Remove a schedule |
| `run_script` | Execute a tool script in the container |
| `save_tool` | Save a new tool script for future use |
| `get_state` / `set_state` | Read/write from the Postgres KV store |
| `get_status` | Uptime, skill count, schedule count |

### How Self-Extension Works

**Example: "Give me a daily summary of my GitHub PRs every morning"**

1. Agent writes `tools/github-prs.sh` (a shell script using `gh pr list`)
2. Agent writes `skills/morning-pr-check.md`:
   ```markdown
   ---
   name: morning-pr-check
   description: Check GitHub PRs and send a morning summary
   schedule: "0 9 * * *"
   ---
   Run the github-prs tool for my repos, summarize open PRs,
   and notify me if any need review.
   ```
3. System creates Absurd schedule `skill:morning-pr-check`
4. Every morning at 9am, the skill executes automatically

**Example: "Remind me to buy milk at 5pm"**

1. Agent calls `create_schedule("reminder:buy-milk", "send-message", "0 17 * * *", { params: { chatId, text: "Reminder: buy milk!" } })`
2. At 5pm, Absurd spawns `send-message` which sends the Telegram message
3. If it's a one-shot reminder, the agent can use `sleepUntil` in a spawned task instead

No plugins, no Docker images, no manifests. Just skills, scripts, and schedules.

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
pattern. The user can edit notes from the host via the mounted volume.

### Conversation History (Postgres)

```sql
CREATE SCHEMA IF NOT EXISTS assistant;

CREATE TABLE IF NOT EXISTS assistant.messages (
  id          BIGSERIAL PRIMARY KEY,
  chat_id     BIGINT NOT NULL,
  role        TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'tool')),
  content     TEXT NOT NULL,
  tool_use    JSONB,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_messages_chat
  ON assistant.messages(chat_id, created_at DESC);
```

### State Store (Postgres KV)

Generic key-value store for skills and tools to persist data.

```sql
CREATE TABLE IF NOT EXISTS assistant.state (
  namespace   TEXT NOT NULL,
  key         TEXT NOT NULL,
  value       JSONB NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace, key)
);
```

The `namespace` field scopes state by skill or tool name (e.g.,
`"skill:daily-todo-summary"`, `"tool:github-prs"`).

### Context Window Strategy

When building the Claude prompt:

1. Always include: system instructions + agent notes + tool definitions
2. Include relevant skill descriptions (one-liners for discovery)
3. Fill remaining context with recent messages (newest first)
4. If history exceeds budget, summarize older messages into a "Previously..."
   block (itself a checkpointed step)

## Security Model

### Container Isolation

The entire assistant runs inside a Docker container:

- **Non-root user** inside the container
- **Mounted volumes** for `data/`, `skills/`, `tools/` (persistent, inspectable from host)
- **Network access** limited to Telegram API, Claude API, and Postgres
- **No Docker socket** mounted (no container-in-container)
- **Read-only application code** (only data/, skills/, tools/ are writable)

### Blast Radius

If the agent misbehaves, the worst case is:
- It corrupts its own skills/notes → delete the volumes and restart
- It sends unwanted Telegram messages → revoke the bot token
- It writes garbage to Postgres → the assistant schema is isolated from absurd schema

It cannot access the host filesystem, other services, or escape the container.

### Tool Script Safety

- Scripts execute with `timeout 30s` (configurable)
- Scripts run as the non-root container user
- Network access within the container (same as the agent itself)
- The user can inspect/edit tool scripts from the host via the mounted volume

## System Prompt Structure

```
[Base personality and instructions]
[Contents of data/notes.md]
[Available skill descriptions (one-liners)]
[Available tool script descriptions]
[Current date/time]
---
[Dynamically queried state summaries]
---
[Last N messages from conversation history]
```

## Project Structure

```
absurd-assistant/
  src/
    index.ts              # Entry: init Absurd, bot, worker
    config.ts             # Config from env vars
    bot.ts                # grammY bot setup
    tasks/
      handle-message.ts   # Main agent loop task
      execute-skill.ts    # Scheduled skill execution task
      send-message.ts     # Proactive outbound message task
    agent/
      loop.ts             # Claude API agent loop logic
      prompt.ts           # System prompt assembly
      tools.ts            # Built-in tool definitions
      executor.ts         # Tool dispatch (built-in + scripts)
    memory/
      history.ts          # Postgres message history CRUD
      notes.ts            # Agent notes file read/write
      state.ts            # Postgres KV store
    skills/
      manager.ts          # Skill discovery, CRUD, schedule sync
    scripts/
      runner.ts           # Tool script execution (with timeout)
  skills/                  # Skill files (writable volume)
    (agent creates these at runtime)
  tools/                   # Tool scripts (writable volume)
    (agent creates these at runtime)
  data/
    notes.md              # Agent notes (writable volume)
  sql/
    assistant.sql         # Schema for messages + state
  package.json
  tsconfig.json
  Dockerfile
  docker-compose.yml      # Assistant + Postgres
```

### Dependencies

- `absurd-sdk` -- Absurd TypeScript SDK
- `grammy` -- Telegram Bot API
- `@anthropic-ai/sdk` -- Claude API

No `dockerode` or `chokidar` needed. The agent runs inside Docker (managed
externally) and tool scripts run via `child_process.execFile`.

## Deployment

```yaml
# docker-compose.yml
services:
  assistant:
    build: .
    restart: unless-stopped
    volumes:
      - ./data:/app/data
      - ./skills:/app/skills
      - ./tools:/app/tools
    environment:
      - TELEGRAM_BOT_TOKEN
      - ANTHROPIC_API_KEY
      - DATABASE_URL=postgresql://postgres:postgres@db/absurd
    depends_on:
      - db

  db:
    image: postgres:17
    volumes:
      - pgdata:/var/lib/postgresql/data
    environment:
      - POSTGRES_DB=absurd
      - POSTGRES_PASSWORD=postgres

volumes:
  pgdata:
```

## Open Questions

- **Skill sharing**: Should skills be installable from a URL or git repo?
  Could enable a community skill library.
- **Multi-channel**: When adding channels beyond Telegram, should they be
  separate bot instances or a unified adapter layer?
- **Context window management**: What's the right threshold for triggering
  conversation summarization? Token count vs message count?
- **Agent notes consolidation**: Should this be automatic (periodic) or
  user-initiated?
- **Skill approval flow**: Should the agent require user confirmation before
  activating a new scheduled skill? (Send proposed skill via Telegram,
  wait for thumbs-up before creating the schedule.)
