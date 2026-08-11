package examples_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type exampleManifest struct {
	SchemaVersion int `json:"schemaVersion"`
	Repository    string
	ProofSources  []struct {
		ID     string
		Type   string
		Path   string
		Symbol string
	} `json:"proofSources"`
	Examples []struct {
		ID             string
		Owner          string
		SourcePath     string
		Availability   string
		Versions       map[string]string
		OfflineCommand string
		Assertion      string
		WorkflowPath   string
		JobID          string `json:"jobId"`
		Cleanup        string
		LiveGate       any
		ProofIDs       []string `json:"proofIds"`
	} `json:"examples"`
}

func TestDocsExamplesArtifacts(t *testing.T) {
	t.Parallel()

	root := filepath.Clean("..")
	manifestPath := filepath.Join(root, "testdata", "docs", "examples.json")
	raw, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var manifest exampleManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if manifest.SchemaVersion != 1 || manifest.Repository != "foreignloops" {
		t.Fatalf("manifest identity = version %d repository %q", manifest.SchemaVersion, manifest.Repository)
	}
	if len(manifest.Examples) != 3 {
		t.Fatalf("manifest examples = %d, want 3", len(manifest.Examples))
	}

	proofs := make(map[string]bool, len(manifest.ProofSources))
	for _, proof := range manifest.ProofSources {
		if proof.ID == "" || proof.Type == "" || proof.Path == "" {
			t.Fatalf("incomplete proof: %#v", proof)
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(proof.Path))); err != nil {
			t.Fatalf("proof %q path: %v", proof.ID, err)
		}
		proofs[proof.ID] = true
	}
	for _, example := range manifest.Examples {
		if example.ID == "" || example.Owner != "foreignloops" || example.Availability != "source-workspace" {
			t.Fatalf("invalid example identity: %#v", example)
		}
		if example.Versions["github.com/looprig/foreignloops"] != "source-workspace" {
			t.Fatalf("example %q version = %#v", example.ID, example.Versions)
		}
		if example.OfflineCommand != "GOWORK=off go test -race ./..." {
			t.Fatalf("example %q command = %q", example.ID, example.OfflineCommand)
		}
		if example.WorkflowPath != ".github/workflows/docs-examples.yml" || example.JobID != "docs-examples" {
			t.Fatalf("example %q workflow = %q/%q", example.ID, example.WorkflowPath, example.JobID)
		}
		if example.SourcePath == "" || example.Assertion == "" || example.Cleanup == "" || len(example.ProofIDs) == 0 {
			t.Fatalf("example %q has incomplete execution contract", example.ID)
		}
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(example.SourcePath))); err != nil {
			t.Fatalf("example %q source: %v", example.ID, err)
		}
		for _, proofID := range example.ProofIDs {
			if !proofs[proofID] {
				t.Fatalf("example %q references unknown proof %q", example.ID, proofID)
			}
		}
	}
}
