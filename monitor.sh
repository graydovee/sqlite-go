#!/bin/bash
# sqlite-go development monitor
# Checks task completion and retries failed/interrupted tasks

PROJECT_DIR="$HOME/projects/sqlite/sqlite-go"
STATUS_FILE="$PROJECT_DIR/.task-status.json"
LOG_FILE="$PROJECT_DIR/monitor.log"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" >> "$LOG_FILE"; }

# Initialize status file if not exists
if [ ! -f "$STATUS_FILE" ]; then
    echo '{"tasks":[],"last_check":null}' > "$STATUS_FILE"
fi

# Check if a task is complete by looking for its success marker
check_task() {
    local task_name="$1"
    local marker="$PROJECT_DIR/.task-done-$task_name"
    [ -f "$marker" ]
}

# Check if claude code is running for a specific task
check_running() {
    local task_name="$1"
    pgrep -f "claude.*--print.*$task_name" > /dev/null 2>&1
}

# Get list of tasks that need to be done
# Each task: name|description|worktree_branch|files_touched
TASKS=(
    "groupby-fix|Fix GROUP BY remapSourceColumns cursor=-1 bug|fix/groupby|compile/select.go,vdbe/vdbe.go"
    "where-delete-update|Implement WHERE clause for DELETE and UPDATE|fix/where|sqlite/sqlite.go"
    "select-distinct|Implement SELECT DISTINCT using sorter-based dedup|feat/distinct|compile/select.go,vdbe/vdbe.go"
    "limit-offset|Implement LIMIT and OFFSET clauses|feat/limit|compile/select.go,compile/build.go"
    "foreign-key|Implement Foreign Key constraints|feat/fk|sqlite/sqlite.go,compile/ddl.go,compile/select.go"
    "index|Implement Index creation and usage|feat/index|btree/,compile/,sqlite/sqlite.go"
    "attach-detach|Implement ATTACH and DETACH database|feat/attach|sqlite/sqlite.go,vfs/"
    "savepoint|Implement SAVEPOINT nested transactions|feat/savepoint|pager/,sqlite/sqlite.go"
    "analyze|Implement ANALYZE statistics collection|feat/analyze|sqlite/sqlite.go,btree/"
    "fts|Implement FTS full-text search|feat/fts|sqlite/sqlite.go,compile/,functions/"
)

log "=== Monitor check started ==="

# Build project and run tests to verify current state
cd "$PROJECT_DIR"
BUILD_OK=false
TEST_OK=false

if go build ./... 2>/dev/null; then
    BUILD_OK=true
    log "Build: OK"
else
    log "Build: FAILED"
fi

if $BUILD_OK && timeout 120 go test ./tests/ 2>/dev/null; then
    TEST_OK=true
    log "Tests: ALL PASS"
else
    log "Tests: FAILED or TIMEOUT"
fi

# Check each task status
for task_entry in "${TASKS[@]}"; do
    IFS='|' read -r task_name task_desc task_branch task_files <<< "$task_entry"
    
    if check_task "$task_name"; then
        log "Task $task_name: DONE ✓"
        continue
    fi
    
    if check_running "$task_name"; then
        log "Task $task_name: RUNNING..."
        continue
    fi
    
    log "Task $task_name: NOT STARTED or INTERRUPTED - needs attention"
done

log "=== Monitor check complete ==="
echo "Monitor check complete at $(date). See $LOG_FILE for details."
