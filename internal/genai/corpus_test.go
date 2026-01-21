package genai

import (
	"encoding/json"
	"testing"

	otlpCommon "go.opentelemetry.io/proto/otlp/common/v1"
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

// Helper to extract string value from AnyValue
func getStringValue(av *otlpCommon.AnyValue) string {
	if sv := av.GetStringValue(); sv != "" {
		return sv
	}
	return ""
}

// Helper to extract kvlist from AnyValue
func getKvlist(av *otlpCommon.AnyValue) []*otlpCommon.KeyValue {
	if kv := av.GetKvlistValue(); kv != nil {
		return kv.Values
	}
	return nil
}

// Helper to extract array from AnyValue
func getArray(av *otlpCommon.AnyValue) []*otlpCommon.AnyValue {
	if arr := av.GetArrayValue(); arr != nil {
		return arr.Values
	}
	return nil
}

// Helper to find a key in kvlist
func findInKvlist(kvs []*otlpCommon.KeyValue, key string) *otlpCommon.AnyValue {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.Value
		}
	}
	return nil
}

func TestMessagePartToAnyValue_Text(t *testing.T) {
	part := MessagePart{
		Type:    "text",
		Content: "Hello, world!",
	}

	av := part.ToAnyValue()
	kvs := getKvlist(av)

	if kvs == nil {
		t.Fatal("Expected kvlist, got nil")
	}

	// Check type
	typeVal := findInKvlist(kvs, "type")
	if typeVal == nil || getStringValue(typeVal) != "text" {
		t.Errorf("Expected type='text', got %v", typeVal)
	}

	// Check content
	contentVal := findInKvlist(kvs, "content")
	if contentVal == nil || getStringValue(contentVal) != "Hello, world!" {
		t.Errorf("Expected content='Hello, world!', got %v", contentVal)
	}
}

func TestMessagePartToAnyValue_ToolCall(t *testing.T) {
	part := MessagePart{
		Type:      "tool_call",
		ID:        "call_123",
		Name:      "get_weather",
		Arguments: json.RawMessage(`{"location": "Paris"}`),
	}

	av := part.ToAnyValue()
	kvs := getKvlist(av)

	if kvs == nil {
		t.Fatal("Expected kvlist, got nil")
	}

	// Check type
	if typeVal := findInKvlist(kvs, "type"); getStringValue(typeVal) != "tool_call" {
		t.Errorf("Expected type='tool_call'")
	}

	// Check id
	if idVal := findInKvlist(kvs, "id"); getStringValue(idVal) != "call_123" {
		t.Errorf("Expected id='call_123'")
	}

	// Check name
	if nameVal := findInKvlist(kvs, "name"); getStringValue(nameVal) != "get_weather" {
		t.Errorf("Expected name='get_weather'")
	}

	// Check arguments is a kvlist (structured), not a string
	argsVal := findInKvlist(kvs, "arguments")
	if argsVal == nil {
		t.Fatal("Expected arguments to be present")
	}
	argsKvs := getKvlist(argsVal)
	if argsKvs == nil {
		t.Errorf("Expected arguments to be structured kvlist, got %T", argsVal.Value)
	}

	// Check nested location value
	locationVal := findInKvlist(argsKvs, "location")
	if locationVal == nil || getStringValue(locationVal) != "Paris" {
		t.Errorf("Expected arguments.location='Paris', got %v", locationVal)
	}
}

func TestMessageToAnyValue(t *testing.T) {
	msg := Message{
		Role: "user",
		Parts: []MessagePart{
			{Type: "text", Content: "What's the weather?"},
		},
	}

	av := msg.ToAnyValue()
	kvs := getKvlist(av)

	if kvs == nil {
		t.Fatal("Expected kvlist, got nil")
	}

	// Check role
	if roleVal := findInKvlist(kvs, "role"); getStringValue(roleVal) != "user" {
		t.Errorf("Expected role='user'")
	}

	// Check parts is an array
	partsVal := findInKvlist(kvs, "parts")
	if partsVal == nil {
		t.Fatal("Expected parts to be present")
	}
	partsArr := getArray(partsVal)
	if len(partsArr) != 1 {
		t.Errorf("Expected 1 part, got %d", len(partsArr))
	}
}

func TestMessageToAnyValue_WithFinishReason(t *testing.T) {
	msg := Message{
		Role: "assistant",
		Parts: []MessagePart{
			{Type: "text", Content: "The weather is sunny."},
		},
		FinishReason: "stop",
	}

	av := msg.ToAnyValue()
	kvs := getKvlist(av)

	// Check finish_reason is present
	finishVal := findInKvlist(kvs, "finish_reason")
	if finishVal == nil || getStringValue(finishVal) != "stop" {
		t.Errorf("Expected finish_reason='stop', got %v", finishVal)
	}
}

func TestSystemInstructionToAnyValue(t *testing.T) {
	si := SystemInstruction{
		Type:    "text",
		Content: "You are a helpful assistant.",
	}

	av := si.ToAnyValue()
	kvs := getKvlist(av)

	if kvs == nil {
		t.Fatal("Expected kvlist, got nil")
	}

	if typeVal := findInKvlist(kvs, "type"); getStringValue(typeVal) != "text" {
		t.Errorf("Expected type='text'")
	}

	if contentVal := findInKvlist(kvs, "content"); getStringValue(contentVal) != "You are a helpful assistant." {
		t.Errorf("Expected correct content")
	}
}

func TestToolDefinitionToAnyValue(t *testing.T) {
	td := ToolDefinition{
		Type:        "function",
		Name:        "get_weather",
		Description: "Get the current weather",
		Parameters: json.RawMessage(`{
			"type": "object",
			"properties": {
				"location": {"type": "string", "description": "The city"}
			},
			"required": ["location"]
		}`),
	}

	av := td.ToAnyValue()
	kvs := getKvlist(av)

	if kvs == nil {
		t.Fatal("Expected kvlist, got nil")
	}

	// Check type
	if typeVal := findInKvlist(kvs, "type"); getStringValue(typeVal) != "function" {
		t.Errorf("Expected type='function'")
	}

	// Check name
	if nameVal := findInKvlist(kvs, "name"); getStringValue(nameVal) != "get_weather" {
		t.Errorf("Expected name='get_weather'")
	}

	// Check parameters is structured (kvlist), not a string
	paramsVal := findInKvlist(kvs, "parameters")
	if paramsVal == nil {
		t.Fatal("Expected parameters to be present")
	}

	paramsKvs := getKvlist(paramsVal)
	if paramsKvs == nil {
		t.Errorf("Expected parameters to be structured kvlist, got %T", paramsVal.Value)
	}

	// Check nested properties.location structure
	propsVal := findInKvlist(paramsKvs, "properties")
	if propsVal == nil {
		t.Fatal("Expected properties to be present")
	}

	propsKvs := getKvlist(propsVal)
	locationVal := findInKvlist(propsKvs, "location")
	if locationVal == nil {
		t.Fatal("Expected location property to be present")
	}

	locationKvs := getKvlist(locationVal)
	if typeVal := findInKvlist(locationKvs, "type"); getStringValue(typeVal) != "string" {
		t.Errorf("Expected location.type='string'")
	}
}

func TestConvertConversationsToOTelFormat(t *testing.T) {
	conversations := []Conversation{
		{From: "human", Value: "What's the weather in Paris?"},
		{From: "function_call", Value: `{"name": "get_weather", "arguments": {"location": "Paris"}}`},
		{From: "observation", Value: `{"temp": "22C", "condition": "sunny"}`},
		{From: "gpt", Value: "The weather in Paris is 22°C and sunny."},
	}

	inputMsgs, outputMsgs := convertConversationsToOTelFormat(conversations)

	// Should have 3 input messages (human, function_call, observation)
	if len(inputMsgs) != 3 {
		t.Errorf("Expected 3 input messages, got %d", len(inputMsgs))
	}

	// Should have 1 output message (final gpt response)
	if len(outputMsgs) != 1 {
		t.Errorf("Expected 1 output message, got %d", len(outputMsgs))
	}

	// Check input message roles
	if inputMsgs[0].Role != "user" {
		t.Errorf("Expected first input role='user', got '%s'", inputMsgs[0].Role)
	}
	if inputMsgs[1].Role != "assistant" {
		t.Errorf("Expected second input role='assistant', got '%s'", inputMsgs[1].Role)
	}
	if inputMsgs[2].Role != "tool" {
		t.Errorf("Expected third input role='tool', got '%s'", inputMsgs[2].Role)
	}

	// Check tool_call part type
	if inputMsgs[1].Parts[0].Type != "tool_call" {
		t.Errorf("Expected tool_call part type")
	}

	// Check tool_call_response part type
	if inputMsgs[2].Parts[0].Type != "tool_call_response" {
		t.Errorf("Expected tool_call_response part type")
	}

	// Check output message has finish_reason
	if outputMsgs[0].FinishReason != "stop" {
		t.Errorf("Expected output finish_reason='stop', got '%s'", outputMsgs[0].FinishReason)
	}
}

func TestCorpusToolDefinitionToToolDefinition(t *testing.T) {
	ct := CorpusToolDefinition{
		Name:        "search",
		Description: "Search the web",
		Parameters:  json.RawMessage(`{"type": "object"}`),
	}

	td := ct.ToToolDefinition()

	if td.Type != "function" {
		t.Errorf("Expected type='function', got '%s'", td.Type)
	}
	if td.Name != "search" {
		t.Errorf("Expected name='search', got '%s'", td.Name)
	}
}

func TestGenAIAttributesStructuredFormat(t *testing.T) {
	// Create a minimal entry with all fields
	entry := &Entry{
		Conversations: []Conversation{
			{From: "human", Value: "Hello"},
			{From: "gpt", Value: "Hi there!"},
		},
		System: "You are helpful.",
		Tools:  `[{"name": "test", "description": "A test tool", "parameters": {"type": "object"}}]`,
	}

	attrs := GenAIAttributesFromEntry(entry)

	// Find structured attributes and verify they are ArrayValue, not strings
	for _, attr := range attrs {
		switch attr.Key {
		case "gen_ai.input.messages", "gen_ai.output.messages", "gen_ai.system_instructions", "gen_ai.tool.definitions":
			if attr.Value.GetArrayValue() == nil {
				t.Errorf("Expected %s to be ArrayValue, got %T", attr.Key, attr.Value.Value)
			}
		}
	}
}

func TestParseToolDefinitions(t *testing.T) {
	toolsJSON := `[
		{"name": "func1", "description": "First function", "parameters": {"type": "object"}},
		{"name": "func2", "description": "Second function"}
	]`

	tools := parseToolDefinitions(toolsJSON)

	if len(tools) != 2 {
		t.Fatalf("Expected 2 tools, got %d", len(tools))
	}

	if tools[0].Type != "function" {
		t.Errorf("Expected type='function'")
	}
	if tools[0].Name != "func1" {
		t.Errorf("Expected name='func1'")
	}
	if tools[1].Name != "func2" {
		t.Errorf("Expected name='func2'")
	}
}

// TestPrintGenAIStructure is a debug test to visualize the generated structure
// Run with: go test ./internal/genai/... -v -run TestPrintGenAIStructure
func TestPrintGenAIStructure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping debug output test in short mode")
	}

	entry := &Entry{
		Conversations: []Conversation{
			{From: "human", Value: "What's the weather in Paris?"},
			{From: "function_call", Value: `{"name": "get_weather", "arguments": {"location": "Paris"}}`},
			{From: "observation", Value: `{"temp": "22C", "condition": "sunny"}`},
			{From: "gpt", Value: "The weather in Paris is 22°C and sunny."},
		},
		System: "You are a helpful weather assistant.",
		Tools:  `[{"name": "get_weather", "description": "Get weather for a location", "parameters": {"type": "object", "properties": {"location": {"type": "string"}}}}]`,
	}

	attrs := GenAIAttributesFromEntry(entry)

	t.Log("\n=== Generated GenAI Attributes ===")
	for _, attr := range attrs {
		t.Logf("\nKey: %s", attr.Key)
		printAnyValue(t, attr.Value, "  ")
	}
}

func printAnyValue(t *testing.T, av *otlpCommon.AnyValue, indent string) {
	if av == nil {
		t.Logf("%s(nil)", indent)
		return
	}

	switch v := av.Value.(type) {
	case *otlpCommon.AnyValue_StringValue:
		// Truncate long strings
		s := v.StringValue
		if len(s) > 80 {
			s = s[:80] + "..."
		}
		t.Logf("%sstring: %q", indent, s)
	case *otlpCommon.AnyValue_IntValue:
		t.Logf("%sint: %d", indent, v.IntValue)
	case *otlpCommon.AnyValue_DoubleValue:
		t.Logf("%sdouble: %f", indent, v.DoubleValue)
	case *otlpCommon.AnyValue_BoolValue:
		t.Logf("%sbool: %v", indent, v.BoolValue)
	case *otlpCommon.AnyValue_ArrayValue:
		t.Logf("%sarray[%d]:", indent, len(v.ArrayValue.Values))
		for i, item := range v.ArrayValue.Values {
			if i >= 2 {
				t.Logf("%s  ... (%d more items)", indent, len(v.ArrayValue.Values)-i)
				break
			}
			t.Logf("%s  [%d]:", indent, i)
			printAnyValue(t, item, indent+"    ")
		}
	case *otlpCommon.AnyValue_KvlistValue:
		t.Logf("%skvlist[%d]:", indent, len(v.KvlistValue.Values))
		for _, kv := range v.KvlistValue.Values {
			t.Logf("%s  %s:", indent, kv.Key)
			printAnyValue(t, kv.Value, indent+"    ")
		}
	default:
		t.Logf("%sunknown: %T", indent, v)
	}
}
