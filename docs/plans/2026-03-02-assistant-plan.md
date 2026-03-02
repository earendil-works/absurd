# Absurd Assistant Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a self-extending AI personal assistant that uses Absurd as its durable task backbone, Telegram as the communication channel, Claude as the reasoning engine, and markdown skill files as the extensibility mechanism. The entire agent runs inside a Docker container.

**Architecture:** Every Telegram message spawns a durable Absurd task. The agent loop runs as checkpointed steps (Claude API calls cached via `ctx.step()`). The agent can extend itself by writing skill files (markdown behavioral instructions) and tool scripts (executable code). Scheduled behaviors use Absurd's built-in cron system. Everything runs inside a Docker container for security.

**Tech Stack:** TypeScript, Absurd SDK (`absurd-sdk`), grammY (Telegram), `@anthropic-ai/sdk` (Claude), vitest (tests)

**Design doc:** `docs/plans/2026-03-02-assistant-design.md`

---

## Phase 1: Project Foundation

### Task 1: Scaffold the project

**Files:**
- Create: `package.json`
- Create: `tsconfig.json`
- Create: `.gitignore`
- Create: `data/notes.md`
- Create: `sql/assistant.sql`
- Create: `Dockerfile`
- Create: `docker-compose.yml`

**Step 1: Initialize the repo**

```bash
mkdir absurd-assistant && cd absurd-assistant
git init
mkdir -p src/tasks src/agent src/memory src/skills src/scripts skills tools data sql
```

**Step 2: Create package.json**

```json
{
  "name": "absurd-assistant",
  "version": "0.0.1",
  "type": "module",
  "private": true,
  "scripts": {
    "build": "tsc",
    "dev": "node --experimental-strip-types src/index.ts",
    "test": "vitest --run"
  },
  "dependencies": {
    "absurd-sdk": "github:earendil-works/absurd#main",
    "@anthropic-ai/sdk": "^0.39.0",
    "grammy": "^1.35.0",
    "pg": "^8.13.0"
  },
  "devDependencies": {
    "@types/node": "^22.10.0",
    "@types/pg": "^8.11.10",
    "typescript": "^5.9.0",
    "vitest": "^4.0.0"
  }
}
```

**Step 3: Create tsconfig.json**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "Node16",
    "moduleResolution": "Node16",
    "outDir": "dist",
    "rootDir": "src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "declaration": true,
    "resolveJsonModule": true,
    "sourceMap": true
  },
  "include": ["src"],
  "exclude": ["node_modules", "dist"]
}
```

**Step 4: Create .gitignore**

```
node_modules/
dist/
.env
```

**Step 5: Create initial agent notes**

`data/notes.md`:

```markdown
# User Profile
- (no information yet)

# Learned Preferences
- (none yet)

# Ongoing Context
- (none yet)

# Working Memory
- (none yet)
```

**Step 6: Create SQL schema**

`sql/assistant.sql`:

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

CREATE TABLE IF NOT EXISTS assistant.state (
  namespace   TEXT NOT NULL,
  key         TEXT NOT NULL,
  value       JSONB NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (namespace, key)
);
```

**Step 7: Create Dockerfile**

```dockerfile
FROM node:22-alpine
RUN apk add --no-cache curl jq git bash python3
RUN adduser -D assistant
USER assistant
WORKDIR /app
COPY --chown=assistant package*.json ./
RUN npm install --production
COPY --chown=assistant . .
VOLUME ["/app/data", "/app/skills", "/app/tools"]
CMD ["node", "--experimental-strip-types", "src/index.ts"]
```

**Step 8: Create docker-compose.yml**

```yaml
services:
  assistant:
    build: .
    restart: unless-stopped
    volumes:
      - ./data:/app/data
      - ./skills:/app/skills
      - ./tools:/app/tools
    env_file: .env
    environment:
      - DATABASE_URL=postgresql://postgres:postgres@db/absurd
    depends_on:
      db:
        condition: service_healthy

  db:
    image: postgres:17
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./sql:/docker-entrypoint-initdb.d
    environment:
      - POSTGRES_DB=absurd
      - POSTGRES_PASSWORD=postgres
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 2s
      timeout: 5s
      retries: 5

volumes:
  pgdata:
```

**Step 9: Create .env.example**

```
TELEGRAM_BOT_TOKEN=your-telegram-bot-token
ANTHROPIC_API_KEY=sk-ant-your-key
```

**Step 10: Create minimal src/index.ts and verify**

```typescript
console.log("absurd-assistant starting...");
```

```bash
npm install
npx tsc --noEmit
```

**Step 11: Commit**

```bash
git add -A
git commit -m "feat: scaffold absurd-assistant project"
```

---

### Task 2: Config module

**Files:**
- Create: `src/config.ts`
- Test: `src/config.test.ts`

**Step 1: Write the test**

```typescript
// src/config.test.ts
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { loadConfig } from "./config.js";

describe("loadConfig", () => {
  const orig = process.env;
  beforeEach(() => { process.env = { ...orig }; });
  afterEach(() => { process.env = orig; });

  it("throws if TELEGRAM_BOT_TOKEN is missing", () => {
    delete process.env.TELEGRAM_BOT_TOKEN;
    expect(() => loadConfig()).toThrow("TELEGRAM_BOT_TOKEN");
  });

  it("throws if ANTHROPIC_API_KEY is missing", () => {
    process.env.TELEGRAM_BOT_TOKEN = "t";
    delete process.env.ANTHROPIC_API_KEY;
    expect(() => loadConfig()).toThrow("ANTHROPIC_API_KEY");
  });

  it("returns config with defaults", () => {
    process.env.TELEGRAM_BOT_TOKEN = "tg";
    process.env.ANTHROPIC_API_KEY = "sk";
    const c = loadConfig();
    expect(c.telegramToken).toBe("tg");
    expect(c.anthropicApiKey).toBe("sk");
    expect(c.databaseUrl).toContain("postgresql");
    expect(c.queueName).toBe("assistant");
    expect(c.model).toBe("claude-sonnet-4-5-20250929");
  });
});
```

**Step 2: Run test — expected FAIL**

```bash
npx vitest run src/config.test.ts
```

**Step 3: Implement**

```typescript
// src/config.ts
import * as path from "path";

export interface Config {
  telegramToken: string;
  anthropicApiKey: string;
  databaseUrl: string;
  queueName: string;
  notesPath: string;
  skillsDir: string;
  toolsDir: string;
  model: string;
  maxHistoryMessages: number;
  workerConcurrency: number;
  claimTimeout: number;
  scriptTimeout: number;
}

export function loadConfig(): Config {
  return {
    telegramToken: requireEnv("TELEGRAM_BOT_TOKEN"),
    anthropicApiKey: requireEnv("ANTHROPIC_API_KEY"),
    databaseUrl: process.env.DATABASE_URL ?? "postgresql://localhost/absurd",
    queueName: process.env.QUEUE_NAME ?? "assistant",
    notesPath: path.resolve(process.env.NOTES_PATH ?? "data/notes.md"),
    skillsDir: path.resolve(process.env.SKILLS_DIR ?? "skills"),
    toolsDir: path.resolve(process.env.TOOLS_DIR ?? "tools"),
    model: process.env.CLAUDE_MODEL ?? "claude-sonnet-4-5-20250929",
    maxHistoryMessages: parseInt(process.env.MAX_HISTORY_MESSAGES ?? "50", 10),
    workerConcurrency: parseInt(process.env.WORKER_CONCURRENCY ?? "2", 10),
    claimTimeout: parseInt(process.env.CLAIM_TIMEOUT ?? "300", 10),
    scriptTimeout: parseInt(process.env.SCRIPT_TIMEOUT ?? "30", 10),
  };
}

function requireEnv(name: string): string {
  const val = process.env[name];
  if (!val) throw new Error(`Required environment variable ${name} is not set`);
  return val;
}
```

**Step 4: Run test — expected PASS**

**Step 5: Commit**

```bash
git add src/config.ts src/config.test.ts
git commit -m "feat: add config module"
```

---

## Phase 2: Memory Layer

### Task 3: Agent notes (file read/write)

**Files:**
- Create: `src/memory/notes.ts`
- Test: `src/memory/notes.test.ts`

**Step 1: Write the test**

```typescript
// src/memory/notes.test.ts
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { readNotes, writeNotes } from "./notes.js";

describe("agent notes", () => {
  let tmpDir: string;
  let notesPath: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "notes-"));
    notesPath = path.join(tmpDir, "notes.md");
  });
  afterEach(() => fs.rmSync(tmpDir, { recursive: true }));

  it("returns empty string when file missing", async () => {
    expect(await readNotes(notesPath)).toBe("");
  });

  it("reads existing notes", async () => {
    fs.writeFileSync(notesPath, "# Hello\n");
    expect(await readNotes(notesPath)).toBe("# Hello\n");
  });

  it("writes notes", async () => {
    await writeNotes(notesPath, "# New\n");
    expect(fs.readFileSync(notesPath, "utf-8")).toBe("# New\n");
  });

  it("creates parent dirs", async () => {
    const deep = path.join(tmpDir, "a", "b", "notes.md");
    await writeNotes(deep, "# Deep\n");
    expect(fs.readFileSync(deep, "utf-8")).toBe("# Deep\n");
  });
});
```

**Step 2: Run test — expected FAIL**

**Step 3: Implement**

```typescript
// src/memory/notes.ts
import * as fs from "fs/promises";
import * as path from "path";

export async function readNotes(notesPath: string): Promise<string> {
  try {
    return await fs.readFile(notesPath, "utf-8");
  } catch (err: any) {
    if (err.code === "ENOENT") return "";
    throw err;
  }
}

export async function writeNotes(notesPath: string, content: string): Promise<void> {
  await fs.mkdir(path.dirname(notesPath), { recursive: true });
  await fs.writeFile(notesPath, content, "utf-8");
}
```

**Step 4: Run test — expected PASS**

**Step 5: Commit**

```bash
git add src/memory/notes.ts src/memory/notes.test.ts
git commit -m "feat: add agent notes read/write"
```

---

### Task 4: Conversation history + state store

**Files:**
- Create: `src/memory/history.ts`
- Create: `src/memory/state.ts`

**Step 1: Implement history**

```typescript
// src/memory/history.ts
import type { Pool } from "pg";

export interface Message {
  id?: number;
  chatId: number;
  role: "user" | "assistant" | "tool";
  content: string;
  toolUse?: any;
  createdAt?: Date;
}

export async function appendMessage(pool: Pool, msg: Message): Promise<void> {
  await pool.query(
    `INSERT INTO assistant.messages (chat_id, role, content, tool_use)
     VALUES ($1, $2, $3, $4)`,
    [msg.chatId, msg.role, msg.content, msg.toolUse ? JSON.stringify(msg.toolUse) : null],
  );
}

export async function getRecentMessages(
  pool: Pool, chatId: number, limit: number = 50,
): Promise<Message[]> {
  const result = await pool.query(
    `SELECT id, chat_id, role, content, tool_use, created_at
     FROM assistant.messages WHERE chat_id = $1
     ORDER BY created_at DESC LIMIT $2`,
    [chatId, limit],
  );
  return result.rows.reverse().map((r) => ({
    id: r.id, chatId: r.chat_id, role: r.role,
    content: r.content, toolUse: r.tool_use, createdAt: r.created_at,
  }));
}
```

**Step 2: Implement state store**

```typescript
// src/memory/state.ts
import type { Pool } from "pg";

export async function getState(pool: Pool, namespace: string, key: string): Promise<any | null> {
  const r = await pool.query(
    `SELECT value FROM assistant.state WHERE namespace = $1 AND key = $2`,
    [namespace, key],
  );
  return r.rows.length === 0 ? null : r.rows[0].value;
}

export async function setState(pool: Pool, namespace: string, key: string, value: any): Promise<void> {
  await pool.query(
    `INSERT INTO assistant.state (namespace, key, value, updated_at)
     VALUES ($1, $2, $3, now())
     ON CONFLICT (namespace, key) DO UPDATE SET value = $3, updated_at = now()`,
    [namespace, key, JSON.stringify(value)],
  );
}

export async function deleteState(pool: Pool, namespace: string, key: string): Promise<void> {
  await pool.query(
    `DELETE FROM assistant.state WHERE namespace = $1 AND key = $2`,
    [namespace, key],
  );
}

export async function listState(pool: Pool, namespace: string): Promise<Array<{ key: string; value: any }>> {
  const r = await pool.query(
    `SELECT key, value FROM assistant.state WHERE namespace = $1 ORDER BY key`,
    [namespace],
  );
  return r.rows;
}
```

**Step 3: Verify TypeScript compiles**

```bash
npx tsc --noEmit
```

**Step 4: Commit**

```bash
git add src/memory/history.ts src/memory/state.ts
git commit -m "feat: add conversation history and state store"
```

---

## Phase 3: Skills & Scripts

### Task 5: Skill manager

**Files:**
- Create: `src/skills/manager.ts`
- Test: `src/skills/manager.test.ts`

The skill manager reads, creates, updates, and deletes skill files. It also
parses YAML frontmatter to extract metadata.

**Step 1: Write the test**

```typescript
// src/skills/manager.test.ts
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { listSkills, readSkill, writeSkill, deleteSkill, parseSkillFrontmatter } from "./manager.js";

describe("skill manager", () => {
  let tmpDir: string;
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "skills-")); });
  afterEach(() => fs.rmSync(tmpDir, { recursive: true }));

  it("returns empty list for empty dir", async () => {
    expect(await listSkills(tmpDir)).toEqual([]);
  });

  it("lists .md files as skills", async () => {
    fs.writeFileSync(path.join(tmpDir, "foo.md"), "---\nname: foo\n---\n# Foo");
    fs.writeFileSync(path.join(tmpDir, "bar.md"), "---\nname: bar\n---\n# Bar");
    fs.writeFileSync(path.join(tmpDir, "not-a-skill.txt"), "ignore me");
    const skills = await listSkills(tmpDir);
    expect(skills).toHaveLength(2);
    expect(skills.map(s => s.name).sort()).toEqual(["bar", "foo"]);
  });

  it("reads a skill file", async () => {
    fs.writeFileSync(path.join(tmpDir, "test.md"), "---\nname: test\ndescription: A test\n---\n# Body");
    const content = await readSkill(tmpDir, "test");
    expect(content).toContain("# Body");
  });

  it("writes a new skill", async () => {
    await writeSkill(tmpDir, "new-skill", "---\nname: new-skill\n---\n# New");
    expect(fs.existsSync(path.join(tmpDir, "new-skill.md"))).toBe(true);
  });

  it("deletes a skill", async () => {
    fs.writeFileSync(path.join(tmpDir, "del.md"), "---\nname: del\n---\n");
    await deleteSkill(tmpDir, "del");
    expect(fs.existsSync(path.join(tmpDir, "del.md"))).toBe(false);
  });

  it("parses frontmatter", () => {
    const fm = parseSkillFrontmatter("---\nname: test\ndescription: Desc\nschedule: \"0 9 * * *\"\n---\n# Body");
    expect(fm.name).toBe("test");
    expect(fm.description).toBe("Desc");
    expect(fm.schedule).toBe("0 9 * * *");
  });

  it("handles missing frontmatter", () => {
    const fm = parseSkillFrontmatter("# Just a heading\nSome text");
    expect(fm.name).toBeUndefined();
  });
});
```

**Step 2: Run test — expected FAIL**

**Step 3: Implement**

```typescript
// src/skills/manager.ts
import * as fs from "fs/promises";
import * as path from "path";

export interface SkillMeta {
  name?: string;
  description?: string;
  schedule?: string;
  [key: string]: string | undefined;
}

export interface SkillSummary {
  name: string;
  description: string;
  schedule?: string;
  filename: string;
}

export function parseSkillFrontmatter(content: string): SkillMeta {
  const match = content.match(/^---\n([\s\S]*?)\n---/);
  if (!match) return {};
  const meta: SkillMeta = {};
  for (const line of match[1].split("\n")) {
    const colon = line.indexOf(":");
    if (colon === -1) continue;
    const key = line.substring(0, colon).trim();
    let val = line.substring(colon + 1).trim();
    // Strip surrounding quotes
    if ((val.startsWith('"') && val.endsWith('"')) ||
        (val.startsWith("'") && val.endsWith("'"))) {
      val = val.slice(1, -1);
    }
    meta[key] = val;
  }
  return meta;
}

export async function listSkills(skillsDir: string): Promise<SkillSummary[]> {
  let entries: string[];
  try {
    entries = await fs.readdir(skillsDir);
  } catch {
    return [];
  }

  const skills: SkillSummary[] = [];
  for (const entry of entries) {
    if (!entry.endsWith(".md")) continue;
    const content = await fs.readFile(path.join(skillsDir, entry), "utf-8");
    const meta = parseSkillFrontmatter(content);
    skills.push({
      name: meta.name ?? entry.replace(/\.md$/, ""),
      description: meta.description ?? "",
      schedule: meta.schedule,
      filename: entry,
    });
  }
  return skills;
}

export async function readSkill(skillsDir: string, name: string): Promise<string> {
  return await fs.readFile(path.join(skillsDir, `${name}.md`), "utf-8");
}

export async function writeSkill(skillsDir: string, name: string, content: string): Promise<void> {
  await fs.mkdir(skillsDir, { recursive: true });
  await fs.writeFile(path.join(skillsDir, `${name}.md`), content, "utf-8");
}

export async function deleteSkill(skillsDir: string, name: string): Promise<void> {
  await fs.unlink(path.join(skillsDir, `${name}.md`));
}
```

**Step 4: Run test — expected PASS**

**Step 5: Commit**

```bash
git add src/skills/manager.ts src/skills/manager.test.ts
git commit -m "feat: add skill manager with frontmatter parsing"
```

---

### Task 6: Script runner

**Files:**
- Create: `src/scripts/runner.ts`
- Test: `src/scripts/runner.test.ts`

Executes tool scripts inside the container with a timeout. Input is JSON
on stdin, output is captured from stdout.

**Step 1: Write the test**

```typescript
// src/scripts/runner.test.ts
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "fs";
import * as path from "path";
import * as os from "os";
import { runScript, listTools } from "./runner.js";

describe("script runner", () => {
  let tmpDir: string;
  beforeEach(() => { tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "tools-")); });
  afterEach(() => fs.rmSync(tmpDir, { recursive: true }));

  it("executes a bash script with stdin input", async () => {
    const script = path.join(tmpDir, "echo.sh");
    fs.writeFileSync(script, '#!/bin/bash\ncat', { mode: 0o755 });
    const result = await runScript(script, '{"text":"hello"}', 5);
    expect(result).toBe('{"text":"hello"}');
  });

  it("captures stdout", async () => {
    const script = path.join(tmpDir, "greet.sh");
    fs.writeFileSync(script, '#!/bin/bash\necho "hi there"', { mode: 0o755 });
    const result = await runScript(script, "{}", 5);
    expect(result.trim()).toBe("hi there");
  });

  it("throws on timeout", async () => {
    const script = path.join(tmpDir, "slow.sh");
    fs.writeFileSync(script, '#!/bin/bash\nsleep 10', { mode: 0o755 });
    await expect(runScript(script, "{}", 1)).rejects.toThrow();
  });

  it("lists tool scripts", async () => {
    fs.writeFileSync(path.join(tmpDir, "foo.sh"), "#!/bin/bash\n", { mode: 0o755 });
    fs.writeFileSync(path.join(tmpDir, "bar.py"), "#!/usr/bin/env python3\n", { mode: 0o755 });
    fs.writeFileSync(path.join(tmpDir, "readme.md"), "not a tool");
    const tools = await listTools(tmpDir);
    expect(tools).toHaveLength(2);
  });
});
```

**Step 2: Run test — expected FAIL**

**Step 3: Implement**

```typescript
// src/scripts/runner.ts
import { execFile } from "child_process";
import * as fs from "fs/promises";
import * as path from "path";

const SCRIPT_EXTENSIONS = new Set([".sh", ".bash", ".py", ".js", ".ts"]);

export function runScript(
  scriptPath: string,
  input: string,
  timeoutSeconds: number,
): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = execFile(
      scriptPath,
      [],
      {
        timeout: timeoutSeconds * 1000,
        maxBuffer: 1024 * 1024, // 1MB
      },
      (err, stdout, stderr) => {
        if (err) {
          if (stderr) reject(new Error(`Script failed: ${stderr}`));
          else reject(err);
          return;
        }
        resolve(stdout);
      },
    );
    if (child.stdin) {
      child.stdin.write(input);
      child.stdin.end();
    }
  });
}

export async function listTools(
  toolsDir: string,
): Promise<Array<{ name: string; path: string }>> {
  let entries: string[];
  try {
    entries = await fs.readdir(toolsDir);
  } catch {
    return [];
  }

  const tools: Array<{ name: string; path: string }> = [];
  for (const entry of entries) {
    const ext = path.extname(entry);
    if (!SCRIPT_EXTENSIONS.has(ext)) continue;
    tools.push({
      name: entry.replace(/\.[^.]+$/, ""),
      path: path.join(toolsDir, entry),
    });
  }
  return tools;
}

export async function saveTool(
  toolsDir: string,
  name: string,
  content: string,
  extension: string = "sh",
): Promise<string> {
  await fs.mkdir(toolsDir, { recursive: true });
  const filePath = path.join(toolsDir, `${name}.${extension}`);
  await fs.writeFile(filePath, content, { mode: 0o755 });
  return filePath;
}
```

**Step 4: Run test — expected PASS**

**Step 5: Commit**

```bash
git add src/scripts/runner.ts src/scripts/runner.test.ts
git commit -m "feat: add script runner with timeout support"
```

---

## Phase 4: Agent Core

### Task 7: Tool definitions

**Files:**
- Create: `src/agent/tools.ts`
- Test: `src/agent/tools.test.ts`

Defines all built-in tools the agent can use.

**Step 1: Write the test**

```typescript
// src/agent/tools.test.ts
import { describe, it, expect } from "vitest";
import { getBuiltinTools } from "./tools.js";

describe("getBuiltinTools", () => {
  it("returns all introspection tools", () => {
    const tools = getBuiltinTools();
    const names = tools.map(t => t.name);
    expect(names).toContain("read_notes");
    expect(names).toContain("update_notes");
    expect(names).toContain("list_skills");
    expect(names).toContain("create_skill");
    expect(names).toContain("run_script");
    expect(names).toContain("save_tool");
    expect(names).toContain("create_schedule");
    expect(names).toContain("list_schedules");
    expect(names).toContain("get_state");
    expect(names).toContain("set_state");
  });

  it("each tool has proper schema", () => {
    for (const tool of getBuiltinTools()) {
      expect(tool.name).toBeTruthy();
      expect(tool.description).toBeTruthy();
      expect(tool.input_schema.type).toBe("object");
    }
  });
});
```

**Step 2: Run test — expected FAIL**

**Step 3: Implement**

```typescript
// src/agent/tools.ts
import type Anthropic from "@anthropic-ai/sdk";

export type ToolDef = Anthropic.Tool;

export function getBuiltinTools(): ToolDef[] {
  return [
    tool("read_notes", "Read your notes file about the user.", {}),
    tool("update_notes", "Update your notes file. Pass the FULL new content.", {
      content: { type: "string", description: "Full new notes content (markdown)" },
    }, ["content"]),
    tool("list_skills", "List all available skills with descriptions.", {}),
    tool("read_skill", "Read a skill's full content.", {
      name: { type: "string", description: "Skill name" },
    }, ["name"]),
    tool("create_skill", "Create a new skill. Include YAML frontmatter with name, description, and optionally schedule (cron expression).", {
      name: { type: "string", description: "Skill name (used as filename)" },
      content: { type: "string", description: "Full skill content with frontmatter" },
    }, ["name", "content"]),
    tool("update_skill", "Update an existing skill.", {
      name: { type: "string", description: "Skill name" },
      content: { type: "string", description: "Full new skill content" },
    }, ["name", "content"]),
    tool("delete_skill", "Delete a skill and its schedule.", {
      name: { type: "string", description: "Skill name to delete" },
    }, ["name"]),
    tool("run_script", "Execute a tool script. Input is passed as JSON stdin.", {
      name: { type: "string", description: "Tool script name (without extension)" },
      input: { type: "object", description: "JSON input to pass to the script" },
    }, ["name"]),
    tool("save_tool", "Save a new tool script for future use.", {
      name: { type: "string", description: "Tool name (used as filename)" },
      content: { type: "string", description: "Script content (include shebang)" },
      extension: { type: "string", description: "File extension: sh, py, js (default: sh)" },
    }, ["name", "content"]),
    tool("list_tools", "List available tool scripts.", {}),
    tool("list_schedules", "List all active Absurd schedules.", {}),
    tool("create_schedule", "Create a recurring schedule. Cron examples: '0 9 * * *' (daily 9am), '*/30 * * * *' (every 30min), '@every 3600' (hourly).", {
      name: { type: "string", description: "Unique schedule name" },
      task_name: { type: "string", description: "Task to run: 'execute-skill' or 'send-message'" },
      schedule_expr: { type: "string", description: "Cron expression or @every interval" },
      params: { type: "object", description: "Params for the task (e.g. {skillName, chatId} or {chatId, text})" },
    }, ["name", "task_name", "schedule_expr"]),
    tool("delete_schedule", "Delete a schedule.", {
      name: { type: "string", description: "Schedule name" },
    }, ["name"]),
    tool("get_state", "Read a value from the state store.", {
      namespace: { type: "string", description: "Namespace (e.g. skill name)" },
      key: { type: "string", description: "Key" },
    }, ["namespace", "key"]),
    tool("set_state", "Write a value to the state store.", {
      namespace: { type: "string", description: "Namespace" },
      key: { type: "string", description: "Key" },
      value: { description: "JSON value to store" },
    }, ["namespace", "key", "value"]),
    tool("get_status", "Get assistant uptime, skill count, and schedule count.", {}),
  ];
}

function tool(
  name: string,
  description: string,
  properties: Record<string, any>,
  required: string[] = [],
): ToolDef {
  return {
    name,
    description,
    input_schema: { type: "object" as const, properties, required },
  };
}
```

**Step 4: Run test — expected PASS**

**Step 5: Commit**

```bash
git add src/agent/tools.ts src/agent/tools.test.ts
git commit -m "feat: add built-in tool definitions"
```

---

### Task 8: Prompt assembly

**Files:**
- Create: `src/agent/prompt.ts`
- Test: `src/agent/prompt.test.ts`

**Step 1: Write the test**

```typescript
// src/agent/prompt.test.ts
import { describe, it, expect } from "vitest";
import { buildSystemPrompt } from "./prompt.js";

describe("buildSystemPrompt", () => {
  it("includes base instructions", () => {
    const p = buildSystemPrompt({ notes: "", skills: [], tools: [] });
    expect(p).toContain("personal AI assistant");
  });

  it("includes notes", () => {
    const p = buildSystemPrompt({ notes: "Name: Alice", skills: [], tools: [] });
    expect(p).toContain("Name: Alice");
  });

  it("includes skill summaries", () => {
    const p = buildSystemPrompt({
      notes: "",
      skills: [{ name: "todo", description: "Manage TODOs", schedule: "0 9 * * *" }],
      tools: [],
    });
    expect(p).toContain("todo");
    expect(p).toContain("Manage TODOs");
  });

  it("includes date", () => {
    const p = buildSystemPrompt({ notes: "", skills: [], tools: [] });
    expect(p).toMatch(/\d{4}-\d{2}-\d{2}/);
  });
});
```

**Step 2: Run test — expected FAIL**

**Step 3: Implement**

```typescript
// src/agent/prompt.ts
export interface PromptContext {
  notes: string;
  skills: Array<{ name: string; description: string; schedule?: string }>;
  tools: Array<{ name: string }>;
}

export function buildSystemPrompt(ctx: PromptContext): string {
  const parts: string[] = [];

  parts.push(`You are a personal AI assistant communicating through Telegram.
You are helpful, concise, and proactive. You can extend your own capabilities
by creating skills (markdown instruction files) and tool scripts.

When you learn something important about the user, use update_notes to save it.
When the user asks for a recurring behavior, create a skill with a schedule.
When you need to run external commands, save a tool script and run it.

For schedules, use cron expressions:
- "0 9 * * *" = daily at 9:00 AM
- "0 9 * * MON-FRI" = weekdays at 9:00 AM
- "*/30 * * * *" = every 30 minutes
- "@every 3600" = every hour

For scheduled messages, use task "send-message" with params {chatId, text}.
For scheduled skills, use task "execute-skill" with params {skillName, chatId}.

Current date and time: ${new Date().toISOString()}`);

  if (ctx.notes.trim()) {
    parts.push(`## Your Notes\n\n${ctx.notes}`);
  }

  if (ctx.skills.length > 0) {
    const list = ctx.skills.map(s =>
      `- **${s.name}**: ${s.description}${s.schedule ? ` (scheduled: ${s.schedule})` : ""}`
    ).join("\n");
    parts.push(`## Available Skills\n\n${list}`);
  }

  if (ctx.tools.length > 0) {
    const list = ctx.tools.map(t => `- ${t.name}`).join("\n");
    parts.push(`## Available Tool Scripts\n\n${list}`);
  }

  return parts.join("\n\n---\n\n");
}
```

**Step 4: Run test — expected PASS**

**Step 5: Commit**

```bash
git add src/agent/prompt.ts src/agent/prompt.test.ts
git commit -m "feat: add system prompt assembly"
```

---

### Task 9: Tool executor

**Files:**
- Create: `src/agent/executor.ts`

Dispatches tool calls to the correct handler. All built-in tools are
handled here.

**Step 1: Implement**

```typescript
// src/agent/executor.ts
import type { Absurd } from "absurd-sdk";
import type { Pool } from "pg";
import { readNotes, writeNotes } from "../memory/notes.js";
import { getState, setState } from "../memory/state.js";
import { listSkills, readSkill, writeSkill, deleteSkill } from "../skills/manager.js";
import { runScript, listTools, saveTool } from "../scripts/runner.js";
import * as path from "path";

export interface ExecutorDeps {
  absurd: Absurd;
  pool: Pool;
  notesPath: string;
  skillsDir: string;
  toolsDir: string;
  scriptTimeout: number;
  startedAt: Date;
  defaultChatId: number;
}

export function createExecutor(deps: ExecutorDeps) {
  return async (name: string, input: Record<string, any>): Promise<string> => {
    switch (name) {
      case "read_notes":
        return (await readNotes(deps.notesPath)) || "(notes file is empty)";

      case "update_notes":
        await writeNotes(deps.notesPath, input.content);
        return "Notes updated.";

      case "list_skills": {
        const skills = await listSkills(deps.skillsDir);
        if (skills.length === 0) return "No skills created yet.";
        return skills.map(s =>
          `- **${s.name}**: ${s.description}${s.schedule ? ` [${s.schedule}]` : ""}`
        ).join("\n");
      }

      case "read_skill":
        return await readSkill(deps.skillsDir, input.name);

      case "create_skill": {
        await writeSkill(deps.skillsDir, input.name, input.content);
        // If skill has a schedule, create an Absurd schedule
        const { parseSkillFrontmatter } = await import("../skills/manager.js");
        const meta = parseSkillFrontmatter(input.content);
        if (meta.schedule) {
          await deps.absurd.createSchedule(
            `skill:${input.name}`,
            "execute-skill",
            meta.schedule,
            { params: { skillName: input.name, chatId: deps.defaultChatId } },
          );
          return `Skill "${input.name}" created with schedule: ${meta.schedule}`;
        }
        return `Skill "${input.name}" created.`;
      }

      case "update_skill": {
        await writeSkill(deps.skillsDir, input.name, input.content);
        const { parseSkillFrontmatter: parse } = await import("../skills/manager.js");
        const meta = parse(input.content);
        if (meta.schedule) {
          try {
            await deps.absurd.deleteSchedule(`skill:${input.name}`);
          } catch { /* may not exist */ }
          await deps.absurd.createSchedule(
            `skill:${input.name}`,
            "execute-skill",
            meta.schedule,
            { params: { skillName: input.name, chatId: deps.defaultChatId } },
          );
        }
        return `Skill "${input.name}" updated.`;
      }

      case "delete_skill":
        await deleteSkill(deps.skillsDir, input.name);
        try {
          await deps.absurd.deleteSchedule(`skill:${input.name}`);
        } catch { /* may not exist */ }
        return `Skill "${input.name}" deleted.`;

      case "run_script": {
        const tools = await listTools(deps.toolsDir);
        const tool = tools.find(t => t.name === input.name);
        if (!tool) return `Tool "${input.name}" not found. Available: ${tools.map(t => t.name).join(", ")}`;
        return await runScript(tool.path, JSON.stringify(input.input ?? {}), deps.scriptTimeout);
      }

      case "save_tool": {
        const ext = input.extension ?? "sh";
        const p = await saveTool(deps.toolsDir, input.name, input.content, ext);
        return `Tool saved to ${path.basename(p)}`;
      }

      case "list_tools": {
        const tools = await listTools(deps.toolsDir);
        if (tools.length === 0) return "No tool scripts yet.";
        return tools.map(t => `- ${t.name}`).join("\n");
      }

      case "list_schedules": {
        const schedules = await deps.absurd.listSchedules();
        if (schedules.length === 0) return "No active schedules.";
        return schedules.map(s =>
          `- **${s.scheduleName}** → ${s.taskName} (${s.scheduleExpr})${s.enabled ? "" : " [DISABLED]"} next: ${s.nextRunAt?.toISOString() ?? "N/A"}`
        ).join("\n");
      }

      case "create_schedule": {
        const r = await deps.absurd.createSchedule(
          input.name, input.task_name, input.schedule_expr,
          { params: input.params },
        );
        return `Schedule "${r.scheduleName}" created. Next: ${r.nextRunAt.toISOString()}`;
      }

      case "delete_schedule":
        await deps.absurd.deleteSchedule(input.name);
        return `Schedule "${input.name}" deleted.`;

      case "get_state": {
        const val = await getState(deps.pool, input.namespace, input.key);
        return val === null ? "(not set)" : JSON.stringify(val);
      }

      case "set_state":
        await setState(deps.pool, input.namespace, input.key, input.value);
        return "State saved.";

      case "get_status": {
        const uptime = Math.floor((Date.now() - deps.startedAt.getTime()) / 1000);
        const skills = await listSkills(deps.skillsDir);
        const tools = await listTools(deps.toolsDir);
        const schedules = await deps.absurd.listSchedules();
        return [`Uptime: ${uptime}s`, `Skills: ${skills.length}`, `Tools: ${tools.length}`, `Schedules: ${schedules.length}`].join("\n");
      }

      default:
        return `Unknown tool: ${name}`;
    }
  };
}
```

**Step 2: Verify compiles**

```bash
npx tsc --noEmit
```

**Step 3: Commit**

```bash
git add src/agent/executor.ts
git commit -m "feat: add tool executor for all built-in tools"
```

---

### Task 10: Agent loop

**Files:**
- Create: `src/agent/loop.ts`

The core agent loop: checkpointed Claude API calls with tool dispatch.

**Step 1: Implement**

```typescript
// src/agent/loop.ts
import Anthropic from "@anthropic-ai/sdk";
import type { TaskContext } from "absurd-sdk";
import type { Pool } from "pg";
import type { ToolDef } from "./tools.js";
import { readNotes } from "../memory/notes.js";
import { getRecentMessages, appendMessage } from "../memory/history.js";
import { listSkills } from "../skills/manager.js";
import { listTools } from "../scripts/runner.js";
import { buildSystemPrompt } from "./prompt.js";

const MAX_TURNS = 20;

export interface AgentDeps {
  anthropic: Anthropic;
  pool: Pool;
  model: string;
  notesPath: string;
  skillsDir: string;
  toolsDir: string;
  maxHistory: number;
  tools: ToolDef[];
  executeTool: (name: string, input: Record<string, any>) => Promise<string>;
}

export async function runAgentLoop(
  ctx: TaskContext,
  chatId: number,
  userMessage: string,
  deps: AgentDeps,
): Promise<string> {
  // Load context (checkpointed)
  const context = await ctx.step("load-context", async () => {
    const [notes, history, skills, tools] = await Promise.all([
      readNotes(deps.notesPath),
      getRecentMessages(deps.pool, chatId, deps.maxHistory),
      listSkills(deps.skillsDir),
      listTools(deps.toolsDir),
    ]);
    return { notes, history, skills, tools };
  });

  const systemPrompt = buildSystemPrompt({
    notes: context.notes as string,
    skills: (context.skills as any[]).map(s => ({ name: s.name, description: s.description, schedule: s.schedule })),
    tools: (context.tools as any[]).map(t => ({ name: t.name })),
  });

  // Build messages array
  const messages: Anthropic.MessageParam[] = [];
  for (const msg of context.history as any[]) {
    if (msg.role === "user" || msg.role === "assistant") {
      messages.push({ role: msg.role, content: msg.content });
    }
  }
  messages.push({ role: "user", content: userMessage });

  let reply = "";

  for (let turn = 0; turn < MAX_TURNS; turn++) {
    const resp = await ctx.step(`agent-turn-${turn}`, async () => {
      const r = await deps.anthropic.messages.create({
        model: deps.model,
        max_tokens: 4096,
        system: systemPrompt,
        messages,
        tools: deps.tools,
      });
      return { content: r.content, stopReason: r.stop_reason };
    });

    const content = resp.content as Anthropic.ContentBlock[];
    messages.push({ role: "assistant", content });

    if ((resp.stopReason as string) !== "tool_use") {
      reply = content
        .filter((b): b is Anthropic.TextBlock => b.type === "text")
        .map(b => b.text)
        .join("\n");
      break;
    }

    // Execute tool calls
    const toolBlocks = content.filter(
      (b): b is Anthropic.ToolUseBlock => b.type === "tool_use",
    );
    const results: Anthropic.ToolResultBlockParam[] = [];
    for (const tb of toolBlocks) {
      const result = await ctx.step(`tool-${turn}-${tb.name}`, () =>
        deps.executeTool(tb.name, tb.input as Record<string, any>),
      );
      results.push({ type: "tool_result", tool_use_id: tb.id, content: result as string });
    }
    messages.push({ role: "user", content: results });
  }

  // Persist messages
  await ctx.step("persist", async () => {
    await appendMessage(deps.pool, { chatId, role: "user", content: userMessage });
    if (reply) {
      await appendMessage(deps.pool, { chatId, role: "assistant", content: reply });
    }
    return true;
  });

  return reply;
}
```

**Step 2: Verify compiles**

```bash
npx tsc --noEmit
```

**Step 3: Commit**

```bash
git add src/agent/loop.ts
git commit -m "feat: add checkpointed agent loop"
```

---

## Phase 5: Tasks, Bot & Entry Point

### Task 11: Absurd tasks

**Files:**
- Create: `src/tasks/handle-message.ts`
- Create: `src/tasks/execute-skill.ts`
- Create: `src/tasks/send-message.ts`

**Step 1: Implement all three tasks**

```typescript
// src/tasks/handle-message.ts
import type { Absurd, TaskContext } from "absurd-sdk";
import type Anthropic from "@anthropic-ai/sdk";
import type { Pool } from "pg";
import type { Bot } from "grammy";
import { runAgentLoop } from "../agent/loop.js";
import { getBuiltinTools } from "../agent/tools.js";
import type { Config } from "../config.js";

export interface HandleMessageDeps {
  anthropic: Anthropic;
  pool: Pool;
  bot: Bot;
  config: Config;
  executeTool: (name: string, input: Record<string, any>) => Promise<string>;
}

export function registerHandleMessage(absurd: Absurd, deps: HandleMessageDeps): void {
  absurd.registerTask(
    { name: "handle-message" },
    async (params: { chatId: number; text: string }, ctx: TaskContext) => {
      const reply = await runAgentLoop(ctx, params.chatId, params.text, {
        anthropic: deps.anthropic,
        pool: deps.pool,
        model: deps.config.model,
        notesPath: deps.config.notesPath,
        skillsDir: deps.config.skillsDir,
        toolsDir: deps.config.toolsDir,
        maxHistory: deps.config.maxHistoryMessages,
        tools: getBuiltinTools(),
        executeTool: deps.executeTool,
      });

      if (reply) {
        await ctx.step("send-reply", async () => {
          await deps.bot.api.sendMessage(params.chatId, reply);
          return true;
        });
      }

      return { reply };
    },
  );
}
```

```typescript
// src/tasks/execute-skill.ts
import type { Absurd, TaskContext } from "absurd-sdk";
import type Anthropic from "@anthropic-ai/sdk";
import type { Pool } from "pg";
import type { Bot } from "grammy";
import { readSkill } from "../skills/manager.js";
import { runAgentLoop } from "../agent/loop.js";
import { getBuiltinTools } from "../agent/tools.js";
import type { Config } from "../config.js";

export interface ExecuteSkillDeps {
  anthropic: Anthropic;
  pool: Pool;
  bot: Bot;
  config: Config;
  executeTool: (name: string, input: Record<string, any>) => Promise<string>;
}

export function registerExecuteSkill(absurd: Absurd, deps: ExecuteSkillDeps): void {
  absurd.registerTask(
    { name: "execute-skill" },
    async (params: { skillName: string; chatId: number }, ctx: TaskContext) => {
      const skillContent = await ctx.step("read-skill", async () => {
        return await readSkill(deps.config.skillsDir, params.skillName);
      });

      // Run a mini agent loop with the skill instructions as the user message
      const reply = await runAgentLoop(
        ctx,
        params.chatId,
        `Execute the following skill instructions:\n\n${skillContent}`,
        {
          anthropic: deps.anthropic,
          pool: deps.pool,
          model: deps.config.model,
          notesPath: deps.config.notesPath,
          skillsDir: deps.config.skillsDir,
          toolsDir: deps.config.toolsDir,
          maxHistory: 10, // Less history for skill execution
          tools: getBuiltinTools(),
          executeTool: deps.executeTool,
        },
      );

      if (reply) {
        await ctx.step("send-skill-result", async () => {
          await deps.bot.api.sendMessage(params.chatId, reply);
          return true;
        });
      }

      return { skillName: params.skillName, reply };
    },
  );
}
```

```typescript
// src/tasks/send-message.ts
import type { Absurd, TaskContext } from "absurd-sdk";
import type { Bot } from "grammy";

export function registerSendMessage(absurd: Absurd, bot: Bot): void {
  absurd.registerTask(
    { name: "send-message" },
    async (params: { chatId: number; text: string }, _ctx: TaskContext) => {
      await bot.api.sendMessage(params.chatId, params.text);
      return { sent: true };
    },
  );
}
```

**Step 2: Verify compiles**

```bash
npx tsc --noEmit
```

**Step 3: Commit**

```bash
git add src/tasks/
git commit -m "feat: add handle-message, execute-skill, and send-message tasks"
```

---

### Task 12: Telegram bot

**Files:**
- Create: `src/bot.ts`

**Step 1: Implement**

```typescript
// src/bot.ts
import { Bot } from "grammy";
import type { Absurd } from "absurd-sdk";

export function createBot(token: string, absurd: Absurd): Bot {
  const bot = new Bot(token);

  bot.on("message:text", async (ctx) => {
    await absurd.spawn("handle-message", {
      chatId: ctx.chat.id,
      text: ctx.message.text,
      messageId: ctx.message.message_id,
    });
  });

  return bot;
}
```

**Step 2: Commit**

```bash
git add src/bot.ts
git commit -m "feat: add Telegram bot"
```

---

### Task 13: Entry point

**Files:**
- Create: `src/index.ts` (overwrite placeholder)

**Step 1: Implement**

```typescript
// src/index.ts
import * as pg from "pg";
import Anthropic from "@anthropic-ai/sdk";
import { Absurd } from "absurd-sdk";
import { loadConfig } from "./config.js";
import { createBot } from "./bot.js";
import { registerHandleMessage } from "./tasks/handle-message.js";
import { registerExecuteSkill } from "./tasks/execute-skill.js";
import { registerSendMessage } from "./tasks/send-message.js";
import { createExecutor } from "./agent/executor.js";
import { listSkills } from "./skills/manager.js";

async function main() {
  const config = loadConfig();
  const startedAt = new Date();

  const pool = new pg.Pool({ connectionString: config.databaseUrl });
  const anthropic = new Anthropic({ apiKey: config.anthropicApiKey });
  const absurd = new Absurd({ db: pool, queueName: config.queueName });

  await absurd.createQueue();

  const bot = createBot(config.telegramToken, absurd);

  // Determine default chat ID (will be set on first message)
  let defaultChatId = 0;
  const origSpawn = absurd.spawn.bind(absurd);
  // Capture chat ID from first message for schedule creation
  bot.on("message:text", (ctx) => {
    if (defaultChatId === 0) defaultChatId = ctx.chat.id;
  });

  const executeTool = createExecutor({
    absurd,
    pool,
    notesPath: config.notesPath,
    skillsDir: config.skillsDir,
    toolsDir: config.toolsDir,
    scriptTimeout: config.scriptTimeout,
    startedAt,
    get defaultChatId() { return defaultChatId; },
  });

  const taskDeps = { anthropic, pool, bot, config, executeTool };
  registerHandleMessage(absurd, taskDeps);
  registerExecuteSkill(absurd, taskDeps);
  registerSendMessage(absurd, bot);

  // Sync skill schedules on startup
  const skills = await listSkills(config.skillsDir);
  for (const skill of skills) {
    if (skill.schedule) {
      try {
        await absurd.createSchedule(
          `skill:${skill.name}`,
          "execute-skill",
          skill.schedule,
          { params: { skillName: skill.name, chatId: defaultChatId } },
        );
      } catch { /* already exists */ }
    }
  }

  const worker = await absurd.startWorker({
    concurrency: config.workerConcurrency,
    claimTimeout: config.claimTimeout,
    onError: (err) => console.error("[worker]", err.message),
  });

  console.log(`absurd-assistant started (queue=${config.queueName})`);

  bot.start({ onStart: () => console.log("Telegram bot connected") });

  const shutdown = async () => {
    console.log("Shutting down...");
    bot.stop();
    await worker.close();
    await pool.end();
    process.exit(0);
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

main().catch((err) => { console.error("Fatal:", err); process.exit(1); });
```

**Step 2: Verify compiles**

```bash
npx tsc --noEmit
```

**Step 3: Commit**

```bash
git add src/index.ts
git commit -m "feat: add entry point wiring everything together"
```

---

## Phase 6: Smoke Test

### Task 14: End-to-end manual test

**Step 1: Set up**

```bash
cp .env.example .env
# Edit .env with real TELEGRAM_BOT_TOKEN and ANTHROPIC_API_KEY
```

**Step 2: Start with docker compose**

```bash
docker compose up --build
```

Wait for:
```
absurd-assistant started (queue=assistant)
Telegram bot connected
```

**Step 3: Test basic conversation**

Send to your Telegram bot: "Hello, what can you do?"

Expected: A response describing its capabilities.

**Step 4: Test notes**

Send: "My name is Alice and I'm in the EST timezone"

Then: "What's my name?"

Expected: It should remember "Alice" (check `data/notes.md` from host).

**Step 5: Test skill creation**

Send: "Create a skill that sends me a motivational quote every morning at 9am"

Expected: Agent creates `skills/morning-quote.md` and an Absurd schedule.
Verify from host: `cat skills/morning-quote.md` and check schedule in Postgres.

**Step 6: Test tool creation**

Send: "Create a tool that checks the weather using curl"

Expected: Agent creates `tools/weather.sh` and can run it.

---

## Summary

| Phase | Tasks | What you get |
|-------|-------|-------------|
| 1: Foundation | 1-2 | Project scaffold, config, schema, Docker setup |
| 2: Memory | 3-4 | Notes file, conversation history, state store |
| 3: Skills & Scripts | 5-6 | Skill manager, script runner |
| 4: Agent Core | 7-10 | Tool defs, prompt, executor, agent loop |
| 5: Tasks + Bot | 11-13 | Absurd tasks, Telegram bot, entry point — **MVP works** |
| 6: Smoke Test | 14 | End-to-end verification |

After Phase 5, you have a fully working self-extending assistant. It can:
- Respond to Telegram messages via Claude
- Remember things about you (notes file)
- Create new skills (markdown files with optional schedules)
- Create and run tool scripts (shell, Python, etc.)
- Schedule recurring behaviors via Absurd's cron system
- All running safely inside a Docker container
