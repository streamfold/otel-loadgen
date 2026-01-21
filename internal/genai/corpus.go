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

// Conversation represents a single conversation turn from the corpus
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

// MessagePart represents a part of a GenAI message
type MessagePart struct {
	Type      string `json:"type"`
	Content   string `json:"content,omitempty"`
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Result    string `json:"result,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

// Message represents a GenAI message in OTel format
type Message struct {
	Role  string        `json:"role"`
	Parts []MessagePart `json:"parts"`
}

// ToolDefinition represents a tool definition in OTel format
type ToolDefinition struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// CorpusToolDefinition represents a tool definition from the corpus (without type field)
type CorpusToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
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

	// Convert conversations to OTel format
	inputMessages, outputMessages := convertConversationsToOTelFormat(entry.Conversations)

	// Calculate token counts based on total message content length
	inputLen := 0
	outputLen := 0
	for _, msg := range inputMessages {
		for _, part := range msg.Parts {
			inputLen += len(part.Content) + len(part.Result) + len(part.Arguments)
		}
	}
	for _, msg := range outputMessages {
		for _, part := range msg.Parts {
			outputLen += len(part.Content) + len(part.Result) + len(part.Arguments)
		}
	}

	// Simulate token counts (rough approximation: ~4 chars per token)
	inputTokens := inputLen / 4
	if inputTokens < 10 {
		inputTokens = 10
	}
	outputTokens := outputLen / 4
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

	// Input messages as JSON string
	// Format: [{"role":"user","parts":[{"type":"text","content":"..."}]}, {"role":"assistant","parts":[{"type":"tool_call","id":"call_123","name":"func","arguments":"{}"}]}, {"role":"tool","parts":[{"type":"tool_call_response","id":"call_123","result":"..."}]}]
	if len(inputMessages) > 0 {
		if jsonBytes, err := json.Marshal(inputMessages); err == nil {
			attrs = append(attrs, stringAttr("gen_ai.input.messages", string(jsonBytes)))
		}
	}

	// Output messages as JSON string
	// Format: [{"role":"assistant","parts":[{"type":"text","content":"..."}]}]
	if len(outputMessages) > 0 {
		if jsonBytes, err := json.Marshal(outputMessages); err == nil {
			attrs = append(attrs, stringAttr("gen_ai.output.messages", string(jsonBytes)))
		}
	}

	// System instructions as JSON array string
	// Format: [{"type":"text","content":"..."}]
	if entry.System != "" {
		systemInstructions := []map[string]string{
			{"type": "text", "content": entry.System},
		}
		if jsonBytes, err := json.Marshal(systemInstructions); err == nil {
			attrs = append(attrs, stringAttr("gen_ai.system_instructions", string(jsonBytes)))
		}
	}

	// Tool definitions as JSON array string
	// Format: [{"type":"function","name":"...","description":"...","parameters":{...}}]
	if entry.Tools != "" {
		toolDefs := parseToolDefinitions(entry.Tools)
		if len(toolDefs) > 0 {
			if jsonBytes, err := json.Marshal(toolDefs); err == nil {
				attrs = append(attrs, stringAttr("gen_ai.tool.definitions", string(jsonBytes)))
			}
		}
	}

	return attrs
}

// parseToolDefinitions parses corpus tool JSON into OTel ToolDefinitions
func parseToolDefinitions(toolsJSON string) []ToolDefinition {
	var corpusTools []CorpusToolDefinition
	if err := json.Unmarshal([]byte(toolsJSON), &corpusTools); err != nil {
		return nil
	}

	result := make([]ToolDefinition, 0, len(corpusTools))
	for _, ct := range corpusTools {
		result = append(result, ToolDefinition{
			Type:        "function",
			Name:        ct.Name,
			Description: ct.Description,
			Parameters:  ct.Parameters,
		})
	}
	return result
}

// convertConversationsToOTelFormat converts corpus conversations to proper GenAI message format
func convertConversationsToOTelFormat(conversations []Conversation) (inputMessages []Message, outputMessages []Message) {
	lastToolCallID := ""

	for i, conv := range conversations {
		switch conv.From {
		case "human":
			// Human messages become user role
			msg := Message{
				Role: "user",
				Parts: []MessagePart{
					{
						Type:    "text",
						Content: conv.Value,
					},
				},
			}
			inputMessages = append(inputMessages, msg)

		case "gpt":
			// GPT messages become assistant role - check if they contain tool calls
			msg := Message{
				Role:  "assistant",
				Parts: []MessagePart{},
			}

			// For simplicity, treat all GPT messages as text content
			// In a real implementation, you'd parse for tool calls
			msg.Parts = append(msg.Parts, MessagePart{
				Type:    "text",
				Content: conv.Value,
			})

			// Check if this is the last message in the conversation
			if i == len(conversations)-1 {
				outputMessages = append(outputMessages, msg)
			} else {
				inputMessages = append(inputMessages, msg)
			}

		case "function_call":
			// Function calls become assistant tool_call messages
			lastToolCallID = fmt.Sprintf("call_%d", rand.Int63())
			msg := Message{
				Role: "assistant",
				Parts: []MessagePart{
					{
						Type:      "tool_call",
						ID:        lastToolCallID,
						Name:      "function_name", // Would parse from conv.Value in real implementation
						Arguments: conv.Value,
					},
				},
			}
			inputMessages = append(inputMessages, msg)

		case "observation":
			// Observations become tool response messages
			msg := Message{
				Role: "tool",
				Parts: []MessagePart{
					{
						Type:   "tool_call_response",
						ID:     lastToolCallID,
						Result: conv.Value,
					},
				},
			}
			inputMessages = append(inputMessages, msg)
		}
	}

	return inputMessages, outputMessages
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
