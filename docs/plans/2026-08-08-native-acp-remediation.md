# Native ACP Selection and Failure Remediation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Harden native ACP model/effort selection and safe failure projection without breaking exported driver.Event source compatibility or leaking sensitive protocol data.

**Architecture:** Keep configuration validation at the native driver boundary, use provider-specific selection paths for model-only and model-plus-effort requests, and represent model-facing failures through an additional event kind rather than changing the Event struct shape. Classify protocol errors only by traversing standard Go error wrappers and sanitize all projected message text at the final boundary.

**Tech Stack:** Go, standard library errors/context/regexp-style scanning, vendored Harness and ACP contracts, race-enabled Go tests.

---

### Task 1: Reproduce native selection and gateway validation failures

**Files:**
- Test: `driver/acp/*_test.go`
- Test: `backend/*_test.go`

**Step 1: Write failing tests**

Cover harness-managed, legacy/model-only, structured model-only, explicit tuple, invalid effort-without-model, and Gateway effort rejection. Use recording/fake ACP clients and assert construction/cleanup behavior for Codex and Claude.

**Step 2: Run focused tests to verify they fail**

Run: `go test ./driver/acp ./backend -run 'Native|Model|Effort|Gateway'`

Expected: model-only construction rejects or uses the paired selector path, and invalid Gateway effort is accepted.

**Step 3: Implement minimal provider-specific selection and validation**

Use immutable Codex `WithModel` for model-only and `WithModelEffort` only for complete tuples. For Claude, select the model alone for model-only and then set effort for tuples. Reject effort with an empty model and Gateway effort.

**Step 4: Run focused tests to verify they pass**

Run: `go test -race ./driver/acp ./backend`

Expected: PASS.

### Task 2: Restore Event source compatibility and safe failure kind

**Files:**
- Modify: `driver/event.go` (or current Event definition)
- Modify: `driver/acp/*`
- Modify: `backend/*`
- Test: corresponding package tests

**Step 1: Write failing compatibility and mapper tests**

Compile/use unkeyed Event literals and assert ordinary errors map to generic `KindError` while projected safe failures use the new compatibility-preserving kind.

**Step 2: Run focused tests to verify they fail**

Run: `go test ./driver/acp ./backend`

Expected: unkeyed literals fail to compile or model-facing classification is represented by the removed field.

**Step 3: Implement minimal enum-based representation**

Remove the appended public Event field, add a new Kind value, and update producers/mappers/tests.

**Step 4: Run focused tests to verify they pass**

Run: `go test -race ./driver/acp ./backend`

Expected: PASS.

### Task 3: Harden protocol error discovery and direct-message sanitization

**Files:**
- Modify: error projection/sanitization implementation
- Test: adversarial ACP/backend tests

**Step 1: Write failing adversarial tests**

Cover custom `As(any) bool` forgery, joined secret siblings, standard wrappers, protocol pointer/value forms, direct Message URLs/paths/assignment secrets/bearer values, control/multiline/invalid UTF-8, useful reset wording, and excluded Data/causes/stderr/env/error text.

**Step 2: Run focused tests to verify they fail**

Run: `go test ./driver/acp ./backend -run 'Protocol|Redact|Sanit|Failure|Secret'`

Expected: forged protocol errors are classified as model-facing or forbidden values appear in output.

**Step 3: Implement bounded standard-wrapper traversal and deterministic redaction**

Traverse only direct protocol type assertions plus `Unwrap() error`/`Unwrap() []error`, with cycle/depth/node bounds. Normalize UTF-8 and one line, retain safe quota/reset wording, and redact URLs, paths, and assignment/bearer secrets without echoing values.

**Step 4: Run focused tests to verify they pass**

Run: `go test -race ./driver/acp ./backend`

Expected: PASS.

### Task 4: Full verification and commit

**Step 1: Run required checks**

Run: `go test -race ./...`, `CGO_ENABLED=0 go build -trimpath ./...`, `go vet ./...`, plus gosec/staticcheck when available.

**Step 2: Inspect the diff and status**

Run: `git diff --check; git status --short; git diff --stat`

Expected: only intended worktree changes, no whitespace errors.

**Step 3: Commit**

Run: `git add docs/plans/2026-08-08-native-acp-remediation.md driver backend && git commit -m "fix: harden native ACP selection and failures"`

Expected: commit succeeds with the requested message.
