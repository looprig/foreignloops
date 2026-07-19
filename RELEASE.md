# Release handoff

This extraction is ready for an authorized release operator, but it has not been
tagged or pushed. Run the stages below in order. Set every `*_TAG` variable to a
real, unpublished tag chosen by the release owner and every `*_REMOTE` variable
to an authorized Git remote. Do not substitute a commit, pseudo-version, or local
filesystem replacement for a required tag.

The paths below name the extraction worktrees used for verification. Use the
corresponding clean release branches if these worktrees have already been
integrated.

## 1. Release Harness first

The Harness release must contain `pkg/foreign` and must not contain
`pkg/foreignloop`, a concrete foreign backend, or a dependency on
`github.com/looprig/foreignloops`.

```sh
HARNESS_DIR=/Users/ipotter/code/looprig/harness/.worktrees/foreignloop-extraction/harness
HARNESS_TAG=...       # required: release-owner-selected real tag
HARNESS_REMOTE=...    # required: authorized push remote

cd "$HARNESS_DIR"
test -d pkg/foreign
test ! -e pkg/foreignloop
! rg -n 'github.com/looprig/foreignloops' --glob '*.go' --glob 'go.mod' .

# Prepare release metadata from the already tagged dependency versions. These
# development replacements are removed on the release branch, not in the
# extraction worktree before authorization.
go mod edit -dropreplace=github.com/looprig/core
go mod edit -dropreplace=github.com/looprig/inference
go mod edit -dropreplace=github.com/looprig/storage
GOWORK=off go mod tidy
GOWORK=off go mod vendor
! rg -n '^[[:space:]]*replace([[:space:]]|\()' go.mod

GOWORK=off go test -count=1 -race ./...
GOWORK=off go test -count=1 -tags integration -race ./...
CGO_ENABLED=0 GOWORK=off go build -trimpath ./...
make lint
go mod verify
# Requires explicit approval for dependency-metadata network disclosure, or an
# approved offline Go vulnerability database:
go tool govulncheck ./...
git diff --check
git add go.mod go.sum vendor
git commit -m 'build: prepare Harness release metadata'
test -z "$(git status --short)"
```

Harness commit `f4406c2` fixes the 13 staticcheck and two gosec findings found
during secure-gate remediation. The full race suite, integration race suite,
CGO-disabled trimpath build, lint gate, and `go mod verify` pass at that commit.
The official `govulncheck` remains a release gate: it was not run because its
network request can disclose private module, package, and dependency metadata,
and that disclosure was not approved. Run it only after explicit privacy
approval, or configure and use an approved offline vulnerability database.

Only after every command above, including the approved vulnerability scan,
passes may an authorized operator run:

```sh
git tag "$HARNESS_TAG"
git push "$HARNESS_REMOTE" "$HARNESS_TAG"
```

Confirm that the tag is visible from the module proxy or release environment
before proceeding.

## 2. Release foreignloop against the real Harness tag

Do not remove the development replacements until the Harness tag above exists
and resolves without local source.

```sh
FOREIGNLOOP_DIR=/Users/ipotter/code/looprig/harness/.worktrees/foreignloop-extraction/foreignloop
FOREIGNLOOP_TAG=...       # required: release-owner-selected real tag
FOREIGNLOOP_REMOTE=...    # required: configured and authorized push remote

cd "$FOREIGNLOOP_DIR"
go mod edit -require="github.com/looprig/harness@$HARNESS_TAG"
go mod edit -dropreplace=github.com/looprig/harness
go mod edit -dropreplace=github.com/looprig/inference
go mod edit -dropreplace=github.com/looprig/storage
GOWORK=off go mod tidy
GOWORK=off make vendor
! rg -n '^[[:space:]]*replace([[:space:]]|\()' go.mod

make root-check
make build
make test GOFLAGS='-mod=vendor -count=1'
go test -count=1 -tags integration -race ./...
go test ./driver/claude -run '^$' -fuzz '^FuzzDecodeStreamLine$' -fuzztime=30s
go test ./driver/claude -run '^$' -fuzz '^FuzzDecodeTranscriptLine$' -fuzztime=30s
go test ./driver/codex -run '^$' -fuzz '^FuzzDecodeLine$' -fuzztime=30s
make secure
git diff --check
git add go.mod go.sum vendor
git commit -m 'build: prepare foreignloop release metadata'
test -z "$(git status --short)"
```

Only an authorized operator may then run:

```sh
git tag "$FOREIGNLOOP_TAG"
git push "$FOREIGNLOOP_REMOTE" "$FOREIGNLOOP_TAG"
```

The extraction worktree currently has no configured foreignloop remote; the
release owner must select and configure one rather than guessing a destination.
Confirm that the tag resolves remotely before proceeding.

## 3. Verify the tagged pair from the tests module

Create `go.release.mod` and `go.release.sum` only after both real tags resolve.
Never commit synthetic release files or let the release target fall back to the
development `go.mod`.

```sh
TESTS_DIR=/Users/ipotter/code/looprig/harness/.worktrees/foreignloop-extraction/tests

cd "$TESTS_DIR"
cp go.mod go.release.mod
go mod edit -modfile=go.release.mod -require="github.com/looprig/harness@$HARNESS_TAG"
go mod edit -modfile=go.release.mod -require="github.com/looprig/foreignloops@$FOREIGNLOOP_TAG"
go mod edit -modfile=go.release.mod -dropreplace=github.com/looprig/core
go mod edit -modfile=go.release.mod -dropreplace=github.com/looprig/inference
go mod edit -modfile=go.release.mod -dropreplace=github.com/looprig/storage
go mod edit -modfile=go.release.mod -dropreplace=github.com/looprig/harness
go mod edit -modfile=go.release.mod -dropreplace=github.com/looprig/foreignloops
go mod edit -modfile=go.release.mod -dropreplace=github.com/looprig/fsstore
go mod edit -modfile=go.release.mod -dropreplace=github.com/looprig/mcp
GOWORK=off go mod tidy -modfile=go.release.mod
make release-check
git add go.release.mod go.release.sum
git commit -m 'test: pin foreignloop release integration modules'
```

The current tests development module requires `github.com/looprig/mcp v0.0.0`
through a local replacement, and that repository currently has no real tag. A
no-local-source `go.release.mod` therefore also requires an authorized published
MCP version (or a separately reviewed removal of that dependency). Do not invent
that version. Until the Harness, foreignloop, and required MCP tags exist,
`make release-check` is expected to stop with `go.release.mod is not prepared`.

The release guard checks the requested modfile before running `go test`. It
rejects relative, absolute, `file:` and other unversioned filesystem replacement
targets, and it never silently reads the development modfile.

## 4. Migrate product composition roots

Product repositories are outside this extraction and are not modified here.
After the tagged-pair release suite passes, migrate each authorized product
composition root to:

1. construct a provider with `claude.NewAgent` or `codex.NewAgent`;
2. build `backend.Config` with the agent, workspace, permission posture, and SID
   mode; and
3. install `backend.BuildWith(cfg)` and `backend.BuildRestoredWith(cfg)` through
   the Harness rig options.

Run each product's full tests before committing. Product edits, commits, tags,
and pushes require separate authorization.

## Task 20 evidence

The verification record at
`/private/tmp/foreignloop-extraction-verification.txt` was started on 2026-07-18
against Harness `d390ccc`, foreignloop `7080f51`, and tests `e02c8e7`, then
updated after Harness remediation commit `f4406c2`:

- Harness full race, integration race, CGO-disabled trimpath build, lint, and
  module verification passed after all 13 staticcheck and two gosec findings
  were fixed.
- Harness official `govulncheck` remains unexecuted pending explicit privacy
  approval for network metadata disclosure or an approved offline database.
- foreignloop root/build/fresh race/integration, three 30-second fuzz targets,
  secure checks, and supported/unsupported cross-builds passed.
- tests `make check GOFLAGS='-count=1'` passed.
- tests `make release-check` stopped as expected because no real release modfile
  exists; it did not substitute local source.

No tag or push is part of this handoff document's preparation.
