package genai

import (
	"testing"
)

func TestLoadCorpus(t *testing.T) {
	corpus, err := LoadCorpus("../../contrib/apigen-mt_5k.json.gz")
	if err != nil {
		t.Fatalf("Failed to load corpus: %v", err)
	}

	if corpus.Size() == 0 {
		t.Fatal("Corpus is empty")
	}

	t.Logf("Loaded corpus with %d entries", corpus.Size())
}

func TestGenAIAttributes(t *testing.T) {
	corpus, err := LoadCorpus("../../contrib/apigen-mt_5k.json.gz")
	if err != nil {
		t.Fatalf("Failed to load corpus: %v", err)
	}

	attrs := corpus.GenAIAttributes()
	if len(attrs) == 0 {
		t.Fatal("No attributes generated")
	}

	t.Logf("Generated %d attributes", len(attrs))

	// Check for expected attribute keys
	expectedKeys := map[string]bool{
		"gen_ai.conversation.id":     false,
		"gen_ai.operation.name":      false,
		"gen_ai.provider.name":       false,
		"gen_ai.request.model":       false,
		"gen_ai.response.model":      false,
		"gen_ai.usage.input_tokens":  false,
		"gen_ai.usage.output_tokens": false,
		"gen_ai.request.temperature": false,
		"gen_ai.request.max_tokens":  false,
		"gen_ai.response.id":         false,
	}

	for _, attr := range attrs {
		if _, ok := expectedKeys[attr.Key]; ok {
			expectedKeys[attr.Key] = true
		}
		t.Logf("  %s", attr.Key)
	}

	for key, found := range expectedKeys {
		if !found {
			t.Errorf("Expected attribute %s not found", key)
		}
	}
}

func TestNextEntryRoundRobin(t *testing.T) {
	corpus, err := LoadCorpus("../../contrib/apigen-mt_5k.json.gz")
	if err != nil {
		t.Fatalf("Failed to load corpus: %v", err)
	}

	// Get first entry twice to verify round-robin
	entry1 := corpus.NextEntry()
	entry2 := corpus.NextEntry()

	if entry1 == entry2 {
		t.Error("NextEntry should return different entries on consecutive calls")
	}
}
