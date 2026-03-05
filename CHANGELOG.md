# Changelog

This contains the changes between releases.

# Unreleased

# 0.1.0

* Fixed SQL injection vulnerabilities in `absurdctl`.  #68
* Added `emit-event` command to `absurdctl` for publishing queue events from the CLI.  #67
* Fixed TypeScript SDK lease handling to reschedule timers after `heartbeat` and checkpoint writes.
* Fixed Habitat task listing to avoid partial results when database queries time out.
* Added a collapse/expand-all toggle for task payload panels in Habitat.  #71
* Enforced first-write-wins semantics for `emit_event`, so later emits no longer overwrite cached event payloads.
* Improved `absurd.claim_task` to bound cancellation and expired-lease sweep work per claim call.
* Hardened `absurd.extend_claim` to validate input and reject missing, non-running, cancelled, or unclaimed runs.
* Improved checkpoint reads: `get_task_checkpoint_state` now hides pending rows by default, and `get_task_checkpoint_states` is run/attempt-aware.
* Added queue-name byte-length validation and aligned queue-name validation behavior across SQL and clients.
* Added queue indexes for run `claim_expires_at`, wait `task_id`, and event `emitted_at` to improve claim and cleanup performance.
* Prevented deadlocks by aligning lock acquisition order across `cancel_task`, `complete_run`, and `fail_run` paths.

# 0.0.8

* Fixed Habitat sub-path deployment support and hardened prefix handling for UI/API routes.

# 0.0.7

* Added hooks support to the TypeScript SDK.  #62
* Fixed race condition in event handling that could cause lost wakeups.  #61

# 0.0.6

* Added Python SDK.  #26
* Added idempotent task spawning support via `idempotencyKey` parameter.  #58
* Added parameter search/filtering in Habitat task list.  #54
* Fixed Python SDK to be transaction agnostic.  #51
* Improved Habitat UI timeout handling and added auto-refresh.  #56

# 0.0.5

* Added `bindToConnection` method to TypeScript SDK.  #37
* Added support for SSL database connections.  #41
* Fixed `absurdctl spawn-task` command.  #24
* Changed Absurd constructor to accept a config object.  #23
* Fixed small temporary resource leak in TypeScript SDK related to waiting.  #23
* Added support for connection strings in `absurdctl`.  #19
* Changed TypeScript SDK dependencies: made `pg` a peer dependency and moved `typescript` to dev dependencies.  #20
* Added `heartbeat` to extend a claim between checkpoints.  #39
* Added explicit task cancellations.  #40
* Ensure that timeouts on events do not re-trigger the event awaiting.  #45
* Add tests to TypeScript SDK and make `complete`/`fail` internal.  #25

# 0.0.4

* Terminate stuck workers and improve concurrency handling.  #18
* Properly retry tasks which had their claim expire.  #17
* Improved migration command with better error handling and validation.
* Small UI improvements to habitat dashboard.
* Ensure migrations are properly released with versioned SQL files.

# 0.0.3

* Fixed an issue with the TypeScript SDK which caused an incorrect config for CJS.

# 0.0.2

* Published TypeScript SDK to npm.

# 0.0.1

* Initial release
