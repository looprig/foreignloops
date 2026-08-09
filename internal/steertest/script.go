// Package steertest contains a deterministic ACP process fixture for foreign
// loop steering tests. The fixture is intentionally small and script-driven:
// tests control protocol races with named gates, never wall-clock sleeps.
package steertest

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

const (
	defaultAgentName    = "@agentclientprotocol/claude-agent-acp"
	defaultAgentVersion = "0.65.0"
	defaultSessionID    = "steertest-session"
	defaultMaxRecords   = 512
	maxScriptBytes      = 512 << 10
	maxActionTextBytes  = 4096
)

// SteeringOutcome is the typed outcome emitted by a scripted
// _session/steering response. Unknown strings are permitted so callers can
// exercise the driver's fail-closed classification path.
type SteeringOutcome string

const (
	OutcomeInjected       SteeringOutcome = "injected"
	OutcomePromptRequired SteeringOutcome = "promptRequired"
	OutcomeStartedNewTurn SteeringOutcome = "startedNewTurn"
	OutcomeFailed         SteeringOutcome = "failed"
)

// ActionKind identifies one observable process action.
type ActionKind string

const (
	ActionUpdate         ActionKind = "update"
	ActionTerminal       ActionKind = "terminal"
	ActionSteerReply     ActionKind = "steer_reply"
	ActionTransportLoss  ActionKind = "transport_loss"
	ActionWait           ActionKind = "wait"
	ActionSetSessionInfo ActionKind = "session_info"
)

// Aliases make scripts read naturally in integration tests.
const (
	StepUpdate        = ActionUpdate
	StepTerminal      = ActionTerminal
	StepSteerReply    = ActionSteerReply
	StepTransportLoss = ActionTransportLoss
	StepWait          = ActionWait
)

// UpdateAction creates one gated or immediate agent-message update.
func UpdateAction(text, gate string) Action {
	return Action{Kind: ActionUpdate, Text: text, Gate: gate}
}

// TerminalAction creates one prompt terminal response.
func TerminalAction(stopReason, gate string) Action {
	return Action{Kind: ActionTerminal, StopReason: stopReason, Gate: gate}
}

// SteerAction creates one typed steering acknowledgement.
func SteerAction(outcome SteeringOutcome, gate string) Action {
	return Action{Kind: ActionSteerReply, Outcome: outcome, Gate: gate}
}

// TransportLossAction closes the ACP transport at a deterministic gate.
func TransportLossAction(gate string) Action {
	return Action{Kind: ActionTransportLoss, Gate: gate}
}

// Action is one process-side action. Gate, when non-empty, causes the child
// process to announce EventGate and wait until Agent.Release(Gate) is called.
// This gives tests a precise linearization point for prompt/steer/terminal
// races without timers.
type Action struct {
	Kind         ActionKind      `json:"kind"`
	Name         string          `json:"name,omitempty"`
	Gate         string          `json:"gate,omitempty"`
	Text         string          `json:"text,omitempty"`
	Outcome      SteeringOutcome `json:"outcome,omitempty"`
	Reason       string          `json:"reason,omitempty"`
	StopReason   string          `json:"stopReason,omitempty"`
	ErrorCode    int             `json:"errorCode,omitempty"`
	ErrorMessage string          `json:"errorMessage,omitempty"`
}

// Step is a compatibility spelling for Action.
type Step = Action

// PromptScript controls one session/prompt request. Actions execute in order.
// An empty action list means "respond with end_turn".
type PromptScript struct {
	Actions []Action `json:"actions,omitempty"`
	// Steps is accepted as a readable alias for Actions. When both are set,
	// Actions wins.
	Steps []Action `json:"steps,omitempty"`
}

// SteerScript controls one _session/steering request. An empty action list
// means an immediate typed injected reply.
type SteerScript struct {
	Actions []Action `json:"actions,omitempty"`
	Steps   []Action `json:"steps,omitempty"`
}

// Script is the serializable process behavior. It is copied and normalized by
// New, so callers can safely mutate their original after construction.
type Script struct {
	AgentName    string          `json:"agentName,omitempty"`
	AgentVersion string          `json:"agentVersion,omitempty"`
	AgentTitle   string          `json:"agentTitle,omitempty"`
	Metadata     json.RawMessage `json:"metadata,omitempty"`
	SessionID    string          `json:"sessionId,omitempty"`

	// Prompts and Steers are consumed in request order. PromptPlans and
	// Steering are accepted aliases for callers that prefer explicit names.
	Prompts     []PromptScript `json:"prompts,omitempty"`
	PromptPlans []PromptScript `json:"promptPlans,omitempty"`
	Steers      []SteerScript  `json:"steers,omitempty"`
	SteerPlans  []SteerScript  `json:"steerPlans,omitempty"`
	Steering    []SteerScript  `json:"steering,omitempty"`

	// Values advertised in session/new. They make native Claude connector
	// selection usable without a real adapter. Empty values receive stable
	// fixture defaults.
	ModelValues  []string `json:"modelValues,omitempty"`
	EffortValues []string `json:"effortValues,omitempty"`

	// Extra is copied into the fake process environment. Reserved STEERTEST_*
	// names are rejected so fixture control cannot be overridden accidentally.
	Extra map[string]string `json:"extra,omitempty"`

	// MaxRecords bounds the parent-side transcript. Zero selects the default.
	MaxRecords int `json:"maxRecords,omitempty"`
}

// DefaultScript returns the Claude ACP 0.65.0 compatibility profile with
// advertised steering support and a host-owned promptRequired idle behavior.
func DefaultScript() Script {
	return Script{
		AgentName:    defaultAgentName,
		AgentVersion: defaultAgentVersion,
		Metadata:     json.RawMessage(`{"steering":{"supported":true,"idleBehaviors":["promptRequired"]}}`),
		SessionID:    defaultSessionID,
		ModelValues:  []string{"fixture-model", "sonnet", "haiku"},
		EffortValues: []string{"low", "medium", "high"},
	}
}

// ClaudeScript is an alias for DefaultScript.
func ClaudeScript() Script { return DefaultScript() }

// CodexScript returns a current Codex ACP identity. Current Codex remains
// steering-disabled in the production driver, making this useful for queued
// fallback tests.
func CodexScript() Script {
	s := DefaultScript()
	s.AgentName = "@agentclientprotocol/codex-acp"
	s.AgentVersion = "1.1.9"
	return s
}

func normalizeScript(in Script) (Script, error) {
	s := cloneScript(in)
	defaults := DefaultScript()
	if s.AgentName == "" {
		s.AgentName = defaults.AgentName
	}
	if s.AgentVersion == "" {
		s.AgentVersion = defaults.AgentVersion
	}
	if len(s.Metadata) == 0 {
		s.Metadata = append(json.RawMessage(nil), defaults.Metadata...)
	}
	if s.SessionID == "" {
		s.SessionID = defaults.SessionID
	}
	if len(s.ModelValues) == 0 {
		s.ModelValues = append([]string(nil), defaults.ModelValues...)
	}
	if len(s.EffortValues) == 0 {
		s.EffortValues = append([]string(nil), defaults.EffortValues...)
	}
	if s.MaxRecords == 0 {
		s.MaxRecords = defaultMaxRecords
	}
	if s.MaxRecords < 1 || s.MaxRecords > 4096 {
		return Script{}, errors.New("steertest: MaxRecords must be between 1 and 4096")
	}
	if len(s.PromptPlans) > 0 && len(s.Prompts) == 0 {
		s.Prompts = append([]PromptScript(nil), s.PromptPlans...)
	}
	if len(s.SteerPlans) > 0 && len(s.Steers) == 0 {
		s.Steers = append([]SteerScript(nil), s.SteerPlans...)
	}
	if len(s.Steering) > 0 && len(s.Steers) == 0 {
		s.Steers = append([]SteerScript(nil), s.Steering...)
	}
	if err := validateScript(s); err != nil {
		return Script{}, err
	}
	return s, nil
}

func validateScript(s Script) error {
	if len(s.Metadata) > maxActionTextBytes {
		return errors.New("steertest: metadata exceeds 4096 bytes")
	}
	if !json.Valid(s.Metadata) {
		return errors.New("steertest: metadata is not valid JSON")
	}
	for key := range s.Extra {
		if len(key) == 0 || len(key) > 128 || len(s.Extra[key]) > maxActionTextBytes || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(s.Extra[key], '\x00') || strings.HasPrefix(key, "STEERTEST_") {
			return fmt.Errorf("steertest: invalid extra environment key")
		}
	}
	for i, prompt := range s.Prompts {
		if err := validateActions(prompt.actions(), fmt.Sprintf("prompt[%d]", i), false); err != nil {
			return err
		}
	}
	for i, steer := range s.Steers {
		if err := validateActions(steer.actions(), fmt.Sprintf("steer[%d]", i), true); err != nil {
			return err
		}
	}
	encoded, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("steertest: encode script: %w", err)
	}
	if len(encoded) > maxScriptBytes {
		return errors.New("steertest: script exceeds 512 KiB")
	}
	return nil
}

func validateActions(actions []Action, field string, steering bool) error {
	for i, action := range actions {
		if action.Kind == "" {
			return fmt.Errorf("steertest: %s action[%d] kind is required", field, i)
		}
		if len(action.Name) > 128 || len(action.Gate) > 128 {
			return fmt.Errorf("steertest: %s action[%d] name or gate is too long", field, i)
		}
		if len(action.Text) > maxActionTextBytes || len(action.Reason) > maxActionTextBytes || len(action.ErrorMessage) > maxActionTextBytes {
			return fmt.Errorf("steertest: %s action[%d] text exceeds 4096 bytes", field, i)
		}
		switch action.Kind {
		case ActionUpdate, ActionTerminal, ActionWait, ActionSetSessionInfo:
			if steering && action.Kind != ActionWait {
				return fmt.Errorf("steertest: %s action[%d] is not a steering action", field, i)
			}
		case ActionSteerReply:
			if !steering {
				return fmt.Errorf("steertest: %s action[%d] is not a prompt action", field, i)
			}
		case ActionTransportLoss:
		default:
			return fmt.Errorf("steertest: %s action[%d] has unknown kind", field, i)
		}
	}
	return nil
}

func (p PromptScript) actions() []Action {
	if len(p.Actions) > 0 {
		return p.Actions
	}
	return p.Steps
}

func (s SteerScript) actions() []Action {
	if len(s.Actions) > 0 {
		return s.Actions
	}
	return s.Steps
}

func cloneScript(in Script) Script {
	out := in
	out.Metadata = append(json.RawMessage(nil), in.Metadata...)
	out.Prompts = clonePromptScripts(in.Prompts)
	out.PromptPlans = clonePromptScripts(in.PromptPlans)
	out.Steers = cloneSteerScripts(in.Steers)
	out.SteerPlans = cloneSteerScripts(in.SteerPlans)
	out.Steering = cloneSteerScripts(in.Steering)
	out.ModelValues = append([]string(nil), in.ModelValues...)
	out.EffortValues = append([]string(nil), in.EffortValues...)
	if in.Extra != nil {
		out.Extra = make(map[string]string, len(in.Extra))
		for k, v := range in.Extra {
			out.Extra[k] = v
		}
	}
	return out
}

func clonePromptScripts(in []PromptScript) []PromptScript {
	if in == nil {
		return nil
	}
	out := make([]PromptScript, len(in))
	for i, p := range in {
		out[i] = p
		out[i].Actions = append([]Action(nil), p.Actions...)
		out[i].Steps = append([]Action(nil), p.Steps...)
	}
	return out
}

func cloneSteerScripts(in []SteerScript) []SteerScript {
	if in == nil {
		return nil
	}
	out := make([]SteerScript, len(in))
	for i, s := range in {
		out[i] = s
		out[i].Actions = append([]Action(nil), s.Actions...)
		out[i].Steps = append([]Action(nil), s.Steps...)
	}
	return out
}

func scriptBytes(s Script) ([]byte, error) {
	encoded, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	if len(encoded) > maxScriptBytes {
		return nil, errors.New("steertest: script exceeds 512 KiB")
	}
	return encoded, nil
}

// stableKeys returns map keys in deterministic order for redacted transcript
// formatting.
func stableKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
