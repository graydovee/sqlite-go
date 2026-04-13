#!/bin/bash
# sqlite-go task monitor and auto-recovery
# Run via: openclaw cron (every hour)
# 
# This script checks if Claude Code sessions are still running,
# and if not, reports status so the agent can decide whether to retry.

PROJECT_DIR="$HOME/projects/sqlite/sqlite-go"
LOG_FILE="$PROJECT_DIR/.monitor-cron.log"
STATUS_FILE="$PROJECT_DIR/.task-pipeline.json"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"; }

# Check if any claude processes are running
CLAUDE_PROCS=$(pgrep -a claude 2>/dev/null | grep -v "grep" || true)
N_CLAUDE=$(echo "$CLAUDE_PROCS" | grep -c "claude" 2>/dev/null || echo "0")

log "=== Cron check === (running claude processes: $N_CLAUDE)"

# Check worktree status
for wt in /tmp/agent-groupby /tmp/agent-where2; do
    if [ -d "$wt" ]; then
        cd "$wt"
        BRANCH=$(git branch --show-current 2>/dev/null)
        LAST_COMMIT=$(git log --oneline -1 2>/dev/null)
        # Check for uncommitted changes
        CHANGES=$(git diff --stat 2>/dev/null)
        UNCOMMITTED=""
        [ -n "$CHANGES" ] && UNCOMMITTED=" [HAS UNCOMMITTED CHANGES]"
        log "Worktree $wt ($BRANCH): $LAST_COMMIT$UNCOMMITTED"
    else
        log "Worktree $wt: NOT FOUND"
    fi
done

# Check main branch status
cd "$PROJECT_DIR"
MAIN_COMMIT=$(git log --oneline -1 2>/dev/null)
log "Main branch: $MAIN_COMMIT"

# Output summary for the agent
echo "MONITOR_SUMMARY"
echo "Claude processes running: $N_CLAUDE"
echo "Main branch: $MAIN_COMMIT"
if [ -n "$CHANGES" ]; then
    echo "WARNING: Uncommitted changes detected in worktrees"
fi
echo "END_MONITOR"
