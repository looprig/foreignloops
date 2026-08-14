package acp

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/looprig/acp/client"
	"github.com/looprig/acp/launch"
	"github.com/looprig/acp/protocol"
	"github.com/looprig/core/content"
	"github.com/looprig/foreignloops/driver"
)

const (
	claudeAgentACPName = "@agentclientprotocol/claude-agent-acp"
	codexAgentACPName  = "@agentclientprotocol/codex-acp"
)

// claudeSteeringVersions is the closed allowlist of
// @agentclientprotocol/claude-agent-acp versions verified to honor the
// request-side `_meta.steering.idleBehavior: "promptRequired"` opt-in this
// driver sends (see steeringIdleMeta) -- that is, to answer an idle steer
// with outcome "promptRequired" so the host owns the fallback prompt,
// instead of starting an adapter-owned turn the host cannot correlate.
//
// It is a version allowlist rather than a capability check because no ACP
// adapter advertises the idle behavior at all today: the `idleBehaviors`
// array parseSteeringAdvertisement looks for does not exist upstream yet
// (the spec-level home for it is still an open RFD), so in practice every
// real Claude adapter reaches this list. The opt-in itself landed upstream
// in claude-agent-acp 0.64.0 and is present in 0.64.x, 0.65.0, and 0.66.0;
// 0.63.0 and earlier answer an idle steer with "startedNewTurn" and are
// therefore excluded.
//
// Exact match is deliberate in both directions. An unlisted newer version
// falls through to the advertisement path and, finding no advertisement,
// simply loses steering and queues instead -- the safe failure. It must
// never become a bare lower bound: that would admit every future release,
// including one that regressed to an adapter-owned idle turn, on nothing
// but the release being newer. Adding a version here requires checking that
// release's own steering handler.
var claudeSteeringVersions = []string{
	"0.64.0", "0.64.1", "0.64.2", "0.65.0", "0.66.0",
}

// codexSteeringFloor is the lowest @agentclientprotocol/codex-acp version
// whose idle race -- an idle steering attempt starting an adapter-owned turn
// the host cannot correlate with any prompt it issued -- has been verified
// fixed against this driver.
//
// nil means no Codex version has been verified, which is the fail-closed
// default and the current state: every Codex adapter is excluded from
// steering and falls back to a queued prompt. As of codex-acp 1.2.0 the fix
// is still an unmerged upstream pull request; the adapter does not read
// `_meta` off a steer request at all, so the `promptRequired` opt-in this
// driver sends is silently discarded and an idle steer still returns
// "startedNewTurn". Setting this to a version admits that version and every
// later one, so it must only ever be set from a verified upstream fix, never
// from a version merely being newer than the last one known to be broken.
//
// This deliberately replaces an earlier equality test against the
// then-current broken version (1.1.9). Equality made the exclusion evaporate
// on the next routine adapter upgrade -- codex-acp 1.2.0 alone would have
// silently switched a known-broken behavior back on -- the exact fail-open
// shape CLAUDE.md's "fail closed on error or ambiguity" rule exists to
// prevent.
var codexSteeringFloor *launch.CodexVersion

// steeringCapability applies the ACP steering policy at the initialize
// boundary. A capability advertisement is useful only when it proves both
// steering support and a host-owned idle fallback; per-harness rules then
// only ever narrow that, never widen it.
func steeringCapability(harness Harness, metadata client.InitializeMetadata) bool {
	switch harness {
	case HarnessClaudeCode:
		return claudeSteeringCapability(metadata)
	case HarnessCodex:
		return codexSteeringCapability(metadata)
	default:
		return false
	}
}

// claudeSteeringCapability admits an adapter that advertises a host-owned
// idle fallback, plus the closed claudeSteeringVersions allowlist covering
// the releases that honor the opt-in without advertising it. An adapter that
// does not identify itself gets no allowlist entry.
func claudeSteeringCapability(metadata client.InitializeMetadata) bool {
	supported, safeIdle := parseSteeringAdvertisement(metadata.Meta)
	if !supported {
		return false
	}
	if safeIdle {
		return true
	}
	if metadata.AgentInfo == nil {
		return false
	}
	return isExactClaudeProfile(metadata.AgentInfo.Name, metadata.AgentInfo.Version)
}

// codexSteeringCapability is an inclusion test, not an exclusion test: Codex
// steering stays off unless every one of a host-owned idle advertisement,
// the canonical adapter identity, and a version at or above a verified
// codexSteeringFloor holds. Missing identity, an unrecognized name, an
// unparseable version, and an unset floor all fail closed.
//
// Shipping codex-acp advertises `steering.supported` with no idle behavior,
// so it is already excluded by the advertisement check alone; the identity
// and floor checks below exist so that a future adapter which starts
// advertising a host-owned idle fallback still cannot enable steering until
// its version is verified.
func codexSteeringCapability(metadata client.InitializeMetadata) bool {
	supported, safeIdle := parseSteeringAdvertisement(metadata.Meta)
	if !supported || !safeIdle {
		return false
	}
	if metadata.AgentInfo == nil || metadata.AgentInfo.Name != codexAgentACPName {
		return false
	}
	return codexIdleRaceFixed(metadata.AgentInfo.Version)
}

// codexIdleRaceFixed reports whether an advertised codex-acp version is at or
// above the verified floor. It reuses acp/launch's strict version parse -- the
// module that already owns codex-acp version comparison for MinCodexVersion --
// rather than adding a second scheme, so a partial ("1.2"), prerelease
// ("1.2.0-rc1"), or empty advertisement is rejected here exactly as it is
// there.
func codexIdleRaceFixed(advertised string) bool {
	if codexSteeringFloor == nil {
		return false
	}
	version, ok := launch.ParseCodexVersion(advertised)
	if !ok {
		return false
	}
	return !version.Less(*codexSteeringFloor)
}

func parseSteeringAdvertisement(raw json.RawMessage) (supported, safeIdle bool) {
	if len(raw) == 0 {
		return false, false
	}
	var wire struct {
		Steering *struct {
			Supported     bool     `json:"supported"`
			IdleBehaviors []string `json:"idleBehaviors"`
		} `json:"steering"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil || wire.Steering == nil {
		return false, false
	}
	if !wire.Steering.Supported {
		return false, false
	}
	for _, behavior := range wire.Steering.IdleBehaviors {
		if behavior == "promptRequired" {
			return true, true
		}
	}
	return true, false
}

func isExactClaudeProfile(name, version string) bool {
	if name != claudeAgentACPName {
		return false
	}
	for _, allowed := range claudeSteeringVersions {
		if version == allowed {
			return true
		}
	}
	return false
}

func cloneMcpServers(servers []protocol.McpServer) []protocol.McpServer {
	if servers == nil {
		return nil
	}
	cloned := make([]protocol.McpServer, len(servers))
	for i, server := range servers {
		cloned[i] = cloneMcpServer(server)
	}
	return cloned
}

func cloneMcpServer(server protocol.McpServer) protocol.McpServer {
	cloned := protocol.McpServer{}
	if server.HTTP != nil {
		value := *server.HTTP
		value.Headers = cloneMcpHeaders(value.Headers)
		value.Meta = cloneRawMessage(value.Meta)
		cloned.HTTP = &value
	}
	if server.Sse != nil {
		value := *server.Sse
		value.Headers = cloneMcpHeaders(value.Headers)
		value.Meta = cloneRawMessage(value.Meta)
		cloned.Sse = &value
	}
	if server.Stdio != nil {
		value := *server.Stdio
		value.Args = append([]string(nil), value.Args...)
		value.Env = cloneMcpEnv(value.Env)
		value.Meta = cloneRawMessage(value.Meta)
		cloned.Stdio = &value
	}
	return cloned
}

func cloneMcpHeaders(headers []protocol.HTTPHeader) []protocol.HTTPHeader {
	if headers == nil {
		return nil
	}
	cloned := make([]protocol.HTTPHeader, len(headers))
	for i, header := range headers {
		cloned[i] = header
		cloned[i].Meta = cloneRawMessage(header.Meta)
	}
	return cloned
}

func cloneMcpEnv(env []protocol.EnvVariable) []protocol.EnvVariable {
	if env == nil {
		return nil
	}
	cloned := make([]protocol.EnvVariable, len(env))
	for i, variable := range env {
		cloned[i] = variable
		cloned[i].Meta = cloneRawMessage(variable.Meta)
	}
	return cloned
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	cloned := make(json.RawMessage, len(raw))
	copy(cloned, raw)
	return cloned
}

const steeringIdleMeta = `{"steering":{"idleBehavior":"promptRequired"}}`

// normalizeSteering converts the fixed ACP extension result into the closed
// provider-neutral outcome set. Any admitted error or unrecognized result is
// ambiguous and therefore cannot be retried automatically.
func normalizeSteering(result client.SteerResult, callErr error) (driver.SteerResult, error) {
	if steeringErr, ok := callErr.(*client.SteeringError); ok && steeringErr != nil {
		if !result.WriteAdmitted {
			result.WriteAdmitted = steeringErr.WriteAdmitted
		}
		if result.ReceiveSequence == 0 {
			result.ReceiveSequence = steeringErr.ReceiveSequence
		}
		if result.ResponseSequence == 0 {
			result.ResponseSequence = steeringErr.ResponseSequence
		}
	}
	normalized := driver.SteerResult{
		Reason:           result.Reason,
		WriteAdmitted:    result.WriteAdmitted,
		ReceiveSequence:  result.ReceiveSequence,
		ResponseSequence: result.ResponseSequence,
	}
	if callErr != nil {
		if !result.WriteAdmitted || steeringErrorGuaranteesNoDelivery(callErr) {
			normalized.Outcome = driver.SteerOutcomeFallbackRequired
		} else {
			normalized.Outcome = driver.SteerOutcomeDeliveryUnknown
		}
		return normalized, callErr
	}

	switch result.Outcome {
	case client.SteerOutcomeInjected:
		normalized.Outcome = driver.SteerOutcomeInjected
	case client.SteerOutcomePromptRequired, client.SteerOutcomeFailed:
		normalized.Outcome = driver.SteerOutcomeFallbackRequired
	case client.SteerOutcomeStartedNewTurn:
		normalized.Outcome = driver.SteerOutcomeDeliveredUntrackable
	default:
		normalized.Outcome = driver.SteerOutcomeDeliveryUnknown
		return normalized, errors.New("acp: unrecognized steering outcome")
	}
	if err := normalized.Validate(); err != nil {
		normalized.Outcome = driver.SteerOutcomeDeliveryUnknown
		return normalized, fmt.Errorf("acp: malformed steering result: %w", err)
	}
	return normalized, nil
}

func steeringErrorGuaranteesNoDelivery(err error) bool {
	var steeringErr *client.SteeringError
	if !errors.As(err, &steeringErr) || steeringErr == nil {
		return false
	}
	return steeringErr.Code == protocol.ErrorCodeMethodNotFound || steeringErr.Code == protocol.ErrorCodeInvalidParams
}

func steerPromptBlocks(request driver.SteerRequest) ([]protocol.ContentBlock, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	blocks := request.Prompt()
	converted := make([]protocol.ContentBlock, 0, len(blocks))
	for i, block := range blocks {
		switch typed := block.(type) {
		case *content.TextBlock:
			if typed == nil {
				return nil, fmt.Errorf("acp: steering prompt[%d] is nil", i)
			}
			converted = append(converted, protocol.ContentBlock{Text: &protocol.TextContent{Text: typed.Text}})
		case *content.DocumentBlock:
			if typed == nil || typed.Text == "" {
				return nil, fmt.Errorf("acp: steering prompt[%d] document is not textual", i)
			}
			converted = append(converted, protocol.ContentBlock{Text: &protocol.TextContent{Text: typed.Text}})
		default:
			return nil, fmt.Errorf("acp: steering prompt[%d] content is unsupported", i)
		}
	}
	return converted, nil
}
