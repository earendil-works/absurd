# Working With Agents

Absurd works well with coding agents because workflow state lives in Postgres and
can be inspected with `absurdctl`.

The easiest way to make that usable for agents is to install the bundled Absurd
skill.  The skill gives the agent a compact debugging playbook for common
operations such as:

- discovering queues
- listing recent tasks
- dumping a task or run
- spawning work
- retrying failed work
- emitting events to wake sleeping tasks

## Install the Bundled Skill

By default, `absurdctl install-skill` installs into `.agents/skills`:

```bash
absurdctl install-skill
```

That creates:

```text
.agents/skills/absurd/SKILL.md
```

If you want to install into a different skills directory, pass it explicitly:

```bash
absurdctl install-skill .pi/skills
absurdctl install-skill ~/.pi/agent/skills
```

Use `--force` to overwrite an existing installed copy:

```bash
absurdctl install-skill .pi/skills --force
```

## Recommended locations

Common choices are:

- `.agents/skills` — generic project-local agent skills
- `.pi/skills` — project-local skills for pi
- `~/.pi/agent/skills` — user-global pi skills

## How Agents Use It

Once installed, compatible agents can discover the `absurd` skill and load it on
request.  The skill is designed to steer the agent towards `absurdctl` first,
so it inspects queue and task state before digging through application code.

A typical workflow looks like this:

```bash
absurdctl list-queues
absurdctl list-tasks --queue=default --limit=20
absurdctl dump-task --task-id=<task-id>
```

From there the agent can usually decide whether to:

- retry a failed task
- emit an event to wake a sleeping task
- spawn a reproducer task
- inspect the application code for the task implementation
