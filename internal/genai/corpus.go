package genai

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync/atomic"

	otlpCommon "go.opentelemetry.io/proto/otlp/common/v1"
)

// Conversation represents a single conversation turn
type Conversation struct {
	From  string `json:"from"`
	Value string `json:"value"`
}

// Entry represents a single entry in the APIGen corpus
type Entry struct {
	Conversations []Conversation `json:"conversations"`
	Tools         string         `json:"tools"`
	System        string         `json:"system"`
}

// Corpus holds the loaded dataset
type Corpus struct {
	entries []Entry
	idx     atomic.Uint64
}

// Provider names for simulated gen_ai spans
var providerNames = []string{
	"openai",
	"anthropic",
	"google",
	"azure",
	"bedrock",
}

// Model names for simulated gen_ai spans
var modelNames = []string{
	"gpt-4o",
	"gpt-4-turbo",
	"claude-3-5-sonnet",
	"claude-3-opus",
	"gemini-1.5-pro",
	"gemini-1.5-flash",
}

// Operation names for gen_ai spans
var operationNames = []string{
	"chat",
	"completion",
	"embedding",
}

// LoadCorpus loads the APIGen corpus from the specified JSON file.
// If the file has a .gz extension, it will be decompressed automatically.
func LoadCorpus(path string) (*Corpus, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open corpus file: %w", err)
	}
	defer file.Close()

	var decoder *json.Decoder

	if strings.HasSuffix(path, ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzReader.Close()
		decoder = json.NewDecoder(gzReader)
	} else {
		decoder = json.NewDecoder(file)
	}

	var entries []Entry
	if err := decoder.Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to parse corpus JSON: %w", err)
	}

	return &Corpus{
		entries: entries,
	}, nil
}

// Size returns the number of entries in the corpus
func (c *Corpus) Size() int {
	return len(c.entries)
}

// GetEntry returns an entry at the given index (wraps around)
func (c *Corpus) GetEntry(idx int) *Entry {
	return &c.entries[idx%len(c.entries)]
}

// NextEntry returns the next entry in round-robin fashion
func (c *Corpus) NextEntry() *Entry {
	idx := c.idx.Add(1) - 1
	return c.GetEntry(int(idx))
}

// GenAIAttributes generates gen_ai span attributes from a corpus entry
func (c *Corpus) GenAIAttributes() []*otlpCommon.KeyValue {
	entry := c.NextEntry()
	return GenAIAttributesFromEntry(entry)
}

// GenAIAttributesFromEntry generates gen_ai span attributes from a specific entry
func GenAIAttributesFromEntry(entry *Entry) []*otlpCommon.KeyValue {
	attrs := make([]*otlpCommon.KeyValue, 0, 15)

	// Generate conversation ID
	conversationID := fmt.Sprintf("conv-%d", rand.Int63())
	attrs = append(attrs, stringAttr("gen_ai.conversation.id", conversationID))

	// Operation name
	opName := operationNames[rand.Intn(len(operationNames))]
	attrs = append(attrs, stringAttr("gen_ai.operation.name", opName))

	// Provider name
	providerName := providerNames[rand.Intn(len(providerNames))]
	attrs = append(attrs, stringAttr("gen_ai.provider.name", providerName))

	// Model names
	modelName := modelNames[rand.Intn(len(modelNames))]
	attrs = append(attrs, stringAttr("gen_ai.request.model", modelName))
	attrs = append(attrs, stringAttr("gen_ai.response.model", modelName))

	// Extract input and output messages from conversations
	var inputMessages, outputMessages string
	for _, conv := range entry.Conversations {
		switch conv.From {
		case "human":
			if inputMessages == "" {
				inputMessages = conv.Value
			} else {
				inputMessages = conv.Value // Use last human message
			}
		case "gpt":
			outputMessages = conv.Value // Use last gpt message
		}
	}

	// Simulate token counts based on message lengths (rough approximation: ~4 chars per token)
	inputTokens := len(inputMessages) / 4
	if inputTokens < 10 {
		inputTokens = 10
	}
	outputTokens := len(outputMessages) / 4
	if outputTokens < 10 {
		outputTokens = 10
	}

	attrs = append(attrs, intAttr("gen_ai.usage.input_tokens", int64(inputTokens)))
	attrs = append(attrs, intAttr("gen_ai.usage.output_tokens", int64(outputTokens)))

	// Temperature (0.0 - 1.0)
	temperature := rand.Float64()
	attrs = append(attrs, floatAttr("gen_ai.request.temperature", temperature))

	// Max tokens (256 - 4096)
	maxTokens := 256 + rand.Intn(3840)
	attrs = append(attrs, intAttr("gen_ai.request.max_tokens", int64(maxTokens)))

	// Response ID
	responseID := fmt.Sprintf("resp-%d", rand.Int63())
	attrs = append(attrs, stringAttr("gen_ai.response.id", responseID))

	// Input/output messages (last messages from conversation)
	if inputMessages != "" {
		attrs = append(attrs, stringAttr("gen_ai.input.messages", inputMessages))
	}
	if outputMessages != "" {
		attrs = append(attrs, stringAttr("gen_ai.output.messages", outputMessages))
	}

	// System instructions
	if entry.System != "" {
		attrs = append(attrs, stringAttr("gen_ai.system.instructions", entry.System))
	}

	// Tool definitions
	if entry.Tools != "" {
		attrs = append(attrs, stringAttr("gen_ai.tool.definitions", entry.Tools))
	}

	return attrs
}

func stringAttr(key, value string) *otlpCommon.KeyValue {
	return &otlpCommon.KeyValue{
		Key:   key,
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_StringValue{StringValue: value}},
	}
}

func intAttr(key string, value int64) *otlpCommon.KeyValue {
	return &otlpCommon.KeyValue{
		Key:   key,
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: value}},
	}
}

func floatAttr(key string, value float64) *otlpCommon.KeyValue {
	return &otlpCommon.KeyValue{
		Key:   key,
		Value: &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_DoubleValue{DoubleValue: value}},
	}
}
