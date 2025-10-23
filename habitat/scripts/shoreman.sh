#!/bin/bash
# [shoreman](https://github.com/chrismytton/shoreman) is an
# implementation of the **Procfile** format. Inspired by the original
# [foreman](http://ddollar.github.com/foreman/) tool for ruby.

# Make sure that any errors cause the script to exit immediately.
set -eo pipefail
[[ "$TRACE" ]] && set -x

# ## Usage

# Usage message that is displayed when `--help` is given as an argument.
usage() {
  echo "Usage: shoreman [procfile|Procfile] [envfile|.env]"
  echo "Run Procfiles using shell."
  echo
  echo "The shoreman script reads commands from [procfile] and starts up the"
  echo "processes that it describes."
}

# ## Logging

# For logging we want to prefix each entry with the current time, as well
# as the process name. This takes two arguments, the name of the process
# with its index, and then reads data from stdin, formats it, and sends it
# to stdout and dev.log.
log() {
  local index="$2"
  local format="%s %s\t| %s"
  local log_format="%s %s\t| %s"

  # We add colors when output is a terminal. `SHOREMAN_COLORS` can override it.
  if [ -t 1 -o "$SHOREMAN_COLORS" == "always" ] &&
    [ "$SHOREMAN_COLORS" != "never" ]; then
    # Bash colors start from 31 up to 37. We calculate what color the process
    # gets based on its index.
    local color="$((31 + (index % 7)))"
    format="\033[0;${color}m%s %s\t|\033[0m %s"
  fi

  while IFS= read -r data; do
    local timestamp="$(date +"%H:%M:%S")"
    printf "$format\n" "$timestamp" "$1" "$data"
    printf "$log_format\n" "$timestamp" "$1" "$data" >>dev.log
  done
}

# ## Running commands

# When a process is started, we want to keep track of its pid so we can
# `kill` it when the parent process receives a signal, and so we can `wait`
# for it to finish before exiting the parent process.
store_pid() {
  pids="$pids $1"
}

# This starts a command asynchronously and stores its pid in a list for use
# later on in the script. We use setsid to create a new process group.
start_command() {
  # Use setsid to create a new process group for better process management
  if command -v setsid >/dev/null 2>&1; then
    setsid bash -c "$1" 2>&1 | log "$2" "$3" &
  else
    # Fallback for systems without setsid
    bash -c "$1" 2>&1 | log "$2" "$3" &
  fi
  pid="$(jobs -p %%)"
  store_pid "$pid"
}

# ## Reading the .env file

# The .env file needs to be a list of assignments like in a shell script.
# Shell-style comments are permitted.
load_env_file() {
  local env_file=${1:-'.env'}

  # Set a default port before loading the .env file
  export PORT=${PORT:-5000}

  if [[ -f "$env_file" ]]; then
    export $(grep "^[^#]*=.*" "$env_file" | xargs)
  fi
}

# ## Reading the Procfile

# The Procfile needs to be parsed to extract the process names and commands.
# The file is given on stdin, see the `<` at the end of this while loop.
run_procfile() {
  local procfile=${1:-'Procfile'}
  # We give each process an index to track its color. We start with 1,
  # because it corresponds to green which is easier on the eye than red (0).
  local index=1
  while read line || [[ -n "$line" ]]; do
    if [[ -z "$line" ]] || [[ "$line" == \#* ]]; then continue; fi
    local name="${line%%:*}"
    local command="${line#*:[[:space:]]}"
    start_command "$command" "${name}" "$index"
    echo "'${command}' started with pid $pid" | log "${name}" "$index"
    index=$((index + 1))
  done <"$procfile"
}

# ## Cleanup

# When a `SIGINT`, `SIGTERM` or `EXIT` is received, this action is run, killing the
# child processes and their descendants. We use process groups for complete cleanup.
onexit() {
  echo "SIGINT received" >/dev/stderr
  echo "sending SIGTERM to all processes and their children" >/dev/stderr
  echo "$(date +"%H:%M:%S") PROCESS MANAGER SHUTTING DOWN" >>dev.log

  # First, try to kill process groups (negative PID kills the process group)
  for pid in $pids; do
    # Kill the process group if it exists
    if kill -0 -$pid 2>/dev/null; then
      echo "killing process group -$pid" >/dev/stderr
      kill -TERM -$pid 2>/dev/null || true
    fi
    # Also kill the process itself as fallback
    kill -TERM $pid 2>/dev/null || true
  done

  # Give processes time to shut down gracefully
  sleep 2

  # Force kill any remaining processes
  for pid in $pids; do
    # Force kill process group
    if kill -0 -$pid 2>/dev/null; then
      echo "force killing process group -$pid" >/dev/stderr
      kill -KILL -$pid 2>/dev/null || true
    fi
    # Force kill the process itself
    kill -KILL $pid 2>/dev/null || true
  done

  # Remove PID file on exit
  rm -f ".shoreman.pid"
}

main() {
  local procfile="$1"
  local env_file="$2"
  local pidfile=".shoreman.pid"

  # If the `--help` option is given, show the usage message and exit.
  expr -- "$*" : ".*--help" >/dev/null && {
    usage
    exit 0
  }

  # Check if shoreman is already running
  if [[ -f "$pidfile" ]]; then
    local existing_pid=$(cat "$pidfile")
    if kill -0 "$existing_pid" 2>/dev/null; then
      echo "error: services are already running. this is good. the server auto compiles and auto restart, so no action needed." >&2
      exit 1
    fi
    # PID file exists but process is not running, remove stale PID file
    rm -f "$pidfile"
  fi

  # Write our PID to the PID file
  echo $$ >"$pidfile"

  # Move the current log to the previous run log
  if [[ -f dev.log ]]; then
    cp dev.log dev-prev.log
  fi
  >dev.log

  echo "!!! =================================================================" >>dev.log
  echo "$(date +"%H:%M:%S") SHOREMAN STARTED" >>dev.log
  echo "!!! =================================================================" >>dev.log

  load_env_file "$env_file"
  run_procfile "$procfile"

  # Set up signal handling that exits cleanly
  trap 'onexit; rm -f "$pidfile"; exit 0' INT TERM

  # Wait for all processes
  exitcode=0
  for pid in $pids; do
    # Wait for the children to finish executing before exiting.
    wait "${pid}" 2>/dev/null || {
      exitcode=$?
      echo "Process $pid exited with code $exitcode" >/dev/stderr
    }
  done

  # Remove PID file on normal exit
  rm -f "$pidfile"
  exit $exitcode
}

main "$@"
