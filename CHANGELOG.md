# Changelog

This contains the changes between releases.

# Unreleased

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
