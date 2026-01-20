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

// OTel GenAI message format per semantic conventions
// https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-events/

// MessagePart represents a part of a message (text, tool_call, or tool_call_response)
type MessagePart struct {
	Type      string          `json:"type"`
	Content   string          `json:"content,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Result    string          `json:"result,omitempty"`
}

// ToAnyValue converts MessagePart to an OTLP AnyValue kvlist
func (mp MessagePart) ToAnyValue() *otlpCommon.AnyValue {
	kvs := []*otlpCommon.KeyValue{
		{Key: "type", Value: stringValue(mp.Type)},
	}

	if mp.Content != "" {
		kvs = append(kvs, &otlpCommon.KeyValue{Key: "content", Value: stringValue(mp.Content)})
	}
	if mp.ID != "" {
		kvs = append(kvs, &otlpCommon.KeyValue{Key: "id", Value: stringValue(mp.ID)})
	}
	if mp.Name != "" {
		kvs = append(kvs, &otlpCommon.KeyValue{Key: "name", Value: stringValue(mp.Name)})
	}
	if len(mp.Arguments) > 0 {
		if argValue := jsonToAnyValue(mp.Arguments); argValue != nil {
			kvs = append(kvs, &otlpCommon.KeyValue{Key: "arguments", Value: argValue})
		}
	}
	if mp.Result != "" {
		kvs = append(kvs, &otlpCommon.KeyValue{Key: "result", Value: stringValue(mp.Result)})
	}

	return kvlistValue(kvs)
}

// Message represents a single message in OTel GenAI format
type Message struct {
	Role         string        `json:"role"`
	Parts        []MessagePart `json:"parts"`
	FinishReason string        `json:"finish_reason,omitempty"`
}

// ToAnyValue converts Message to an OTLP AnyValue kvlist
func (m Message) ToAnyValue() *otlpCommon.AnyValue {
	kvs := []*otlpCommon.KeyValue{
		{Key: "role", Value: stringValue(m.Role)},
	}

	// Convert parts to array
	partValues := make([]*otlpCommon.AnyValue, 0, len(m.Parts))
	for _, part := range m.Parts {
		partValues = append(partValues, part.ToAnyValue())
	}
	kvs = append(kvs, &otlpCommon.KeyValue{Key: "parts", Value: arrayValue(partValues)})

	if m.FinishReason != "" {
		kvs = append(kvs, &otlpCommon.KeyValue{Key: "finish_reason", Value: stringValue(m.FinishReason)})
	}

	return kvlistValue(kvs)
}

// SystemInstruction represents a system instruction part
type SystemInstruction struct {
	Type    string `json:"type"`
	Content string `json:"content"`
}

// ToAnyValue converts SystemInstruction to an OTLP AnyValue kvlist
func (si SystemInstruction) ToAnyValue() *otlpCommon.AnyValue {
	return kvlistValue([]*otlpCommon.KeyValue{
		{Key: "type", Value: stringValue(si.Type)},
		{Key: "content", Value: stringValue(si.Content)},
	})
}

// ToolDefinition represents a tool definition in OTel format
type ToolDefinition struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// ToAnyValue converts ToolDefinition to an OTLP AnyValue kvlist
func (td ToolDefinition) ToAnyValue() *otlpCommon.AnyValue {
	kvs := []*otlpCommon.KeyValue{
		{Key: "type", Value: stringValue(td.Type)},
		{Key: "name", Value: stringValue(td.Name)},
		{Key: "description", Value: stringValue(td.Description)},
	}
	if len(td.Parameters) > 0 {
		if paramValue := jsonToAnyValue(td.Parameters); paramValue != nil {
			kvs = append(kvs, &otlpCommon.KeyValue{Key: "parameters", Value: paramValue})
		}
	}
	return kvlistValue(kvs)
}

// CorpusToolDefinition represents a tool definition from the corpus (without type field)
type CorpusToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// ToToolDefinition converts corpus format to OTel ToolDefinition
func (ct CorpusToolDefinition) ToToolDefinition() ToolDefinition {
	return ToolDefinition{
		Type:        "function",
		Name:        ct.Name,
		Description: ct.Description,
		Parameters:  ct.Parameters,
	}
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
			inputLen += len(part.Content)
		}
	}
	for _, msg := range outputMessages {
		for _, part := range msg.Parts {
			outputLen += len(part.Content)
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

	// Input messages as structured AnyValue array
	if len(inputMessages) > 0 {
		inputValues := make([]*otlpCommon.AnyValue, 0, len(inputMessages))
		for _, msg := range inputMessages {
			inputValues = append(inputValues, msg.ToAnyValue())
		}
		attrs = append(attrs, arrayAttr("gen_ai.input.messages", inputValues))
	}

	// Output messages as structured AnyValue array
	if len(outputMessages) > 0 {
		outputValues := make([]*otlpCommon.AnyValue, 0, len(outputMessages))
		for _, msg := range outputMessages {
			outputValues = append(outputValues, msg.ToAnyValue())
		}
		attrs = append(attrs, arrayAttr("gen_ai.output.messages", outputValues))
	}

	// System instructions as structured AnyValue array
	if entry.System != "" {
		si := SystemInstruction{Type: "text", Content: entry.System}
		attrs = append(attrs, arrayAttr("gen_ai.system_instructions", []*otlpCommon.AnyValue{
			si.ToAnyValue(),
		}))
	}

	// Tool definitions as structured AnyValue array
	if entry.Tools != "" {
		toolDefs := parseToolDefinitions(entry.Tools)
		if len(toolDefs) > 0 {
			toolValues := make([]*otlpCommon.AnyValue, 0, len(toolDefs))
			for _, td := range toolDefs {
				toolValues = append(toolValues, td.ToAnyValue())
			}
			attrs = append(attrs, arrayAttr("gen_ai.tool.definitions", toolValues))
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
		result = append(result, ct.ToToolDefinition())
	}
	return result
}

// convertConversationsToOTelFormat converts corpus conversations to OTel GenAI message format
func convertConversationsToOTelFormat(conversations []Conversation) (inputMessages []Message, outputMessages []Message) {
	var lastToolCallID string
	toolCallCounter := 0

	for i, conv := range conversations {
		switch conv.From {
		case "human":
			// Human messages become user role with text content
			msg := Message{
				Role: "user",
				Parts: []MessagePart{{
					Type:    "text",
					Content: conv.Value,
				}},
			}
			inputMessages = append(inputMessages, msg)

		case "gpt":
			// GPT messages become assistant role with text content
			msg := Message{
				Role: "assistant",
				Parts: []MessagePart{{
					Type:    "text",
					Content: conv.Value,
				}},
			}
			// Check if this is the last message in the conversation
			if i == len(conversations)-1 {
				msg.FinishReason = "stop"
				outputMessages = append(outputMessages, msg)
			} else {
				inputMessages = append(inputMessages, msg)
			}

		case "function_call":
			// Function calls become assistant role with tool_call part
			toolCallCounter++
			lastToolCallID = fmt.Sprintf("call_%d", toolCallCounter)

			// Parse the function call to extract name and arguments
			var funcCall struct {
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if err := json.Unmarshal([]byte(conv.Value), &funcCall); err == nil {
				msg := Message{
					Role: "assistant",
					Parts: []MessagePart{{
						Type:      "tool_call",
						ID:        lastToolCallID,
						Name:      funcCall.Name,
						Arguments: funcCall.Arguments,
					}},
				}
				inputMessages = append(inputMessages, msg)
			}

		case "observation":
			// Observations become tool role with tool_call_response part
			msg := Message{
				Role: "tool",
				Parts: []MessagePart{{
					Type:   "tool_call_response",
					ID:     lastToolCallID,
					Result: conv.Value,
				}},
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

func arrayAttr(key string, values []*otlpCommon.AnyValue) *otlpCommon.KeyValue {
	return &otlpCommon.KeyValue{
		Key: key,
		Value: &otlpCommon.AnyValue{
			Value: &otlpCommon.AnyValue_ArrayValue{
				ArrayValue: &otlpCommon.ArrayValue{Values: values},
			},
		},
	}
}

func stringValue(s string) *otlpCommon.AnyValue {
	return &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_StringValue{StringValue: s}}
}

func kvlistValue(kvs []*otlpCommon.KeyValue) *otlpCommon.AnyValue {
	return &otlpCommon.AnyValue{
		Value: &otlpCommon.AnyValue_KvlistValue{
			KvlistValue: &otlpCommon.KeyValueList{Values: kvs},
		},
	}
}

func arrayValue(values []*otlpCommon.AnyValue) *otlpCommon.AnyValue {
	return &otlpCommon.AnyValue{
		Value: &otlpCommon.AnyValue_ArrayValue{
			ArrayValue: &otlpCommon.ArrayValue{Values: values},
		},
	}
}

// jsonToAnyValue converts arbitrary JSON (as json.RawMessage or []byte) to a structured AnyValue
func jsonToAnyValue(data json.RawMessage) *otlpCommon.AnyValue {
	if len(data) == 0 {
		return nil
	}

	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		// Fall back to string if parsing fails
		return stringValue(string(data))
	}

	return interfaceToAnyValue(v)
}

// interfaceToAnyValue recursively converts a Go interface{} to an AnyValue
func interfaceToAnyValue(v interface{}) *otlpCommon.AnyValue {
	if v == nil {
		return stringValue("")
	}

	switch val := v.(type) {
	case string:
		return stringValue(val)
	case bool:
		return &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_BoolValue{BoolValue: val}}
	case float64:
		// JSON numbers are always float64
		// Check if it's actually an integer
		if val == float64(int64(val)) {
			return &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_IntValue{IntValue: int64(val)}}
		}
		return &otlpCommon.AnyValue{Value: &otlpCommon.AnyValue_DoubleValue{DoubleValue: val}}
	case []interface{}:
		// Convert array
		values := make([]*otlpCommon.AnyValue, 0, len(val))
		for _, item := range val {
			values = append(values, interfaceToAnyValue(item))
		}
		return arrayValue(values)
	case map[string]interface{}:
		// Convert object to kvlist
		kvs := make([]*otlpCommon.KeyValue, 0, len(val))
		for k, v := range val {
			kvs = append(kvs, &otlpCommon.KeyValue{
				Key:   k,
				Value: interfaceToAnyValue(v),
			})
		}
		return kvlistValue(kvs)
	default:
		// Fall back to string representation
		return stringValue(fmt.Sprintf("%v", val))
	}
}
