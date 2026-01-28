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

func TestMessagePartToOTel_Text(t *testing.T) {
	part := MessagePart{
		Type:    "text",
		Content: "Hello, world!",
	}

	av := part.ToOTel()
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

func TestMessagePartToOTel_ToolCall(t *testing.T) {
	part := MessagePart{
		Type:      "tool_call",
		ID:        "call_123",
		Name:      "get_weather",
		Arguments: `{"location": "Paris"}`,
	}

	av := part.ToOTel()
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

	// Check arguments is a string
	argsVal := findInKvlist(kvs, "arguments")
	if argsVal == nil {
		t.Fatal("Expected arguments to be present")
	}
	if getStringValue(argsVal) != `{"location": "Paris"}` {
		t.Errorf("Expected arguments='%s', got '%s'", `{"location": "Paris"}`, getStringValue(argsVal))
	}
}

func TestMessagePartToOTel_ToolCallResponse(t *testing.T) {
	part := MessagePart{
		Type:   "tool_call_response",
		ID:     "call_123",
		Name:   "get_weather",
		Result: `{"temp": "22C"}`,
	}

	av := part.ToOTel()
	kvs := getKvlist(av)

	if kvs == nil {
		t.Fatal("Expected kvlist, got nil")
	}

	// Check type
	if typeVal := findInKvlist(kvs, "type"); getStringValue(typeVal) != "tool_call_response" {
		t.Errorf("Expected type='tool_call_response'")
	}

	// Check id
	if idVal := findInKvlist(kvs, "id"); getStringValue(idVal) != "call_123" {
		t.Errorf("Expected id='call_123'")
	}

	// Check result
	if resultVal := findInKvlist(kvs, "result"); getStringValue(resultVal) != `{"temp": "22C"}` {
		t.Errorf("Expected result to match")
	}
}

func TestMessageToOTel(t *testing.T) {
	msg := Message{
		Role: "user",
		Parts: []MessagePart{
			{Type: "text", Content: "What's the weather?"},
		},
	}

	av := msg.ToOTel()
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

func TestToolDefinitionToOTel(t *testing.T) {
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

	av := td.ToOTel()
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

	// Check parameters is present as a string (JSON)
	paramsVal := findInKvlist(kvs, "parameters")
	if paramsVal == nil {
		t.Fatal("Expected parameters to be present")
	}

	// Parameters should be a string containing the JSON
	paramsStr := getStringValue(paramsVal)
	if paramsStr == "" {
		t.Errorf("Expected parameters to be a non-empty string")
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

	// Check output message role
	if outputMsgs[0].Role != "assistant" {
		t.Errorf("Expected output role='assistant', got '%s'", outputMsgs[0].Role)
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

func TestMessagesToOTel(t *testing.T) {
	messages := []Message{
		{
			Role: "user",
			Parts: []MessagePart{
				{Type: "text", Content: "Hello"},
			},
		},
		{
			Role: "assistant",
			Parts: []MessagePart{
				{Type: "text", Content: "Hi!"},
			},
		},
	}

	av := MessagesToOTel(messages)

	arr := getArray(av)
	if len(arr) != 2 {
		t.Fatalf("Expected 2 messages in array, got %d", len(arr))
	}

	// Check first message
	msg0Kvs := getKvlist(arr[0])
	if roleVal := findInKvlist(msg0Kvs, "role"); getStringValue(roleVal) != "user" {
		t.Errorf("Expected first message role='user'")
	}

	// Check second message
	msg1Kvs := getKvlist(arr[1])
	if roleVal := findInKvlist(msg1Kvs, "role"); getStringValue(roleVal) != "assistant" {
		t.Errorf("Expected second message role='assistant'")
	}
}

func TestToolDefinitionsToOTel(t *testing.T) {
	toolDefs := []ToolDefinition{
		{Type: "function", Name: "func1", Description: "First"},
		{Type: "function", Name: "func2", Description: "Second"},
	}

	av := ToolDefinitionsToOTel(toolDefs)

	arr := getArray(av)
	if len(arr) != 2 {
		t.Fatalf("Expected 2 tool definitions in array, got %d", len(arr))
	}
}

// Tests for OTel type structure validation

func TestMessagePartToOTel_ReturnsKvlistValue(t *testing.T) {
	part := MessagePart{Type: "text", Content: "Hello"}
	av := part.ToOTel()

	// Verify it's a KvlistValue type
	kvlist := av.GetKvlistValue()
	if kvlist == nil {
		t.Fatalf("Expected KvlistValue, got %T", av.Value)
	}

	// Verify the kvlist contains KeyValue entries
	if len(kvlist.Values) == 0 {
		t.Fatal("Expected non-empty kvlist")
	}

	// Each entry should have Key and Value
	for _, kv := range kvlist.Values {
		if kv.Key == "" {
			t.Error("Expected non-empty Key in KeyValue")
		}
		if kv.Value == nil {
			t.Errorf("Expected non-nil Value for key '%s'", kv.Key)
		}
	}
}

func TestMessageToOTel_ReturnsKvlistWithRoleAndParts(t *testing.T) {
	msg := Message{
		Role: "user",
		Parts: []MessagePart{
			{Type: "text", Content: "Hello"},
		},
	}
	av := msg.ToOTel()

	// Verify it's a KvlistValue
	kvlist := av.GetKvlistValue()
	if kvlist == nil {
		t.Fatalf("Expected KvlistValue, got %T", av.Value)
	}

	// Should have exactly "role" and "parts" keys
	keys := make(map[string]bool)
	for _, kv := range kvlist.Values {
		keys[kv.Key] = true
	}

	if !keys["role"] {
		t.Error("Expected 'role' key in Message kvlist")
	}
	if !keys["parts"] {
		t.Error("Expected 'parts' key in Message kvlist")
	}

	// Verify 'role' is a StringValue
	roleVal := findInKvlist(kvlist.Values, "role")
	if roleVal.GetStringValue() == "" {
		t.Errorf("Expected role to be StringValue, got %T", roleVal.Value)
	}

	// Verify 'parts' is an ArrayValue
	partsVal := findInKvlist(kvlist.Values, "parts")
	if partsVal.GetArrayValue() == nil {
		t.Errorf("Expected parts to be ArrayValue, got %T", partsVal.Value)
	}

	// Verify each part in the array is a KvlistValue
	for i, partVal := range partsVal.GetArrayValue().Values {
		if partVal.GetKvlistValue() == nil {
			t.Errorf("Expected parts[%d] to be KvlistValue, got %T", i, partVal.Value)
		}
	}
}

func TestMessagesToOTel_ReturnsArrayOfKvlists(t *testing.T) {
	messages := []Message{
		{Role: "user", Parts: []MessagePart{{Type: "text", Content: "Q1"}}},
		{Role: "assistant", Parts: []MessagePart{{Type: "text", Content: "A1"}}},
	}
	av := MessagesToOTel(messages)

	// Verify it's an ArrayValue
	arr := av.GetArrayValue()
	if arr == nil {
		t.Fatalf("Expected ArrayValue, got %T", av.Value)
	}

	// Verify each element is a KvlistValue (representing a Message)
	for i, elem := range arr.Values {
		kvlist := elem.GetKvlistValue()
		if kvlist == nil {
			t.Errorf("Expected messages[%d] to be KvlistValue, got %T", i, elem.Value)
		}
	}
}

func TestToolDefinitionToOTel_ReturnsKvlistWithExpectedKeys(t *testing.T) {
	td := ToolDefinition{
		Type:        "function",
		Name:        "test_func",
		Description: "A test function",
		Parameters:  json.RawMessage(`{"type":"object"}`),
	}
	av := td.ToOTel()

	// Verify it's a KvlistValue
	kvlist := av.GetKvlistValue()
	if kvlist == nil {
		t.Fatalf("Expected KvlistValue, got %T", av.Value)
	}

	// Should have expected keys
	expectedKeys := []string{"type", "name", "description", "parameters"}
	for _, key := range expectedKeys {
		val := findInKvlist(kvlist.Values, key)
		if val == nil {
			t.Errorf("Expected key '%s' in ToolDefinition kvlist", key)
		}
	}
}

func TestMessagePartTypes_ReturnCorrectOTelStructure(t *testing.T) {
	tests := []struct {
		name         string
		part         MessagePart
		expectedKeys []string
	}{
		{
			name:         "TextPart",
			part:         MessagePart{Type: "text", Content: "Hello"},
			expectedKeys: []string{"type", "content"},
		},
		{
			name:         "ToolCallPart",
			part:         MessagePart{Type: "tool_call", ID: "id1", Name: "func", Arguments: "{}"},
			expectedKeys: []string{"type", "id", "name", "arguments"},
		},
		{
			name:         "ToolCallResponsePart",
			part:         MessagePart{Type: "tool_call_response", ID: "id1", Name: "func", Result: "ok"},
			expectedKeys: []string{"type", "id", "name", "result"},
		},
		{
			name:         "ThinkingPart",
			part:         MessagePart{Type: "thinking", Content: "Let me think..."},
			expectedKeys: []string{"type", "content"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			av := tt.part.ToOTel()

			// Verify it's a KvlistValue
			kvlist := av.GetKvlistValue()
			if kvlist == nil {
				t.Fatalf("Expected KvlistValue, got %T", av.Value)
			}

			// Verify all expected keys are present and are StringValues
			for _, key := range tt.expectedKeys {
				val := findInKvlist(kvlist.Values, key)
				if val == nil {
					t.Errorf("Expected key '%s' to be present", key)
					continue
				}
				if val.GetStringValue() == "" && key != "content" {
					t.Errorf("Expected '%s' to have a StringValue", key)
				}
			}
		})
	}
}

func TestGenAIAttributesFromEntry_OTelTypeStructure(t *testing.T) {
	entry := &Entry{
		Conversations: []Conversation{
			{From: "human", Value: "Hello"},
			{From: "gpt", Value: "Hi!"},
		},
		System: "Be helpful",
		Tools:  `[{"name": "test", "description": "A test"}]`,
	}

	attrs := GenAIAttributesFromEntry(entry)

	// Build a map for easier lookup
	attrMap := make(map[string]*otlpCommon.AnyValue)
	for _, attr := range attrs {
		attrMap[attr.Key] = attr.Value
	}

	// Verify gen_ai.input.messages is ArrayValue of KvlistValues
	if inputMsgs := attrMap["gen_ai.input.messages"]; inputMsgs != nil {
		arr := inputMsgs.GetArrayValue()
		if arr == nil {
			t.Errorf("gen_ai.input.messages: expected ArrayValue, got %T", inputMsgs.Value)
		} else {
			for i, msg := range arr.Values {
				if msg.GetKvlistValue() == nil {
					t.Errorf("gen_ai.input.messages[%d]: expected KvlistValue, got %T", i, msg.Value)
				}
			}
		}
	} else {
		t.Error("gen_ai.input.messages not found")
	}

	// Verify gen_ai.output.messages is ArrayValue of KvlistValues
	if outputMsgs := attrMap["gen_ai.output.messages"]; outputMsgs != nil {
		arr := outputMsgs.GetArrayValue()
		if arr == nil {
			t.Errorf("gen_ai.output.messages: expected ArrayValue, got %T", outputMsgs.Value)
		} else {
			for i, msg := range arr.Values {
				if msg.GetKvlistValue() == nil {
					t.Errorf("gen_ai.output.messages[%d]: expected KvlistValue, got %T", i, msg.Value)
				}
			}
		}
	} else {
		t.Error("gen_ai.output.messages not found")
	}

	// Verify gen_ai.system_instructions is ArrayValue of KvlistValues (TextParts)
	if sysInstr := attrMap["gen_ai.system_instructions"]; sysInstr != nil {
		arr := sysInstr.GetArrayValue()
		if arr == nil {
			t.Errorf("gen_ai.system_instructions: expected ArrayValue, got %T", sysInstr.Value)
		} else {
			for i, part := range arr.Values {
				kvlist := part.GetKvlistValue()
				if kvlist == nil {
					t.Errorf("gen_ai.system_instructions[%d]: expected KvlistValue, got %T", i, part.Value)
				} else {
					// Verify it has 'type' and 'content' keys
					typeVal := findInKvlist(kvlist.Values, "type")
					if typeVal == nil || typeVal.GetStringValue() != "text" {
						t.Errorf("gen_ai.system_instructions[%d]: expected type='text'", i)
					}
				}
			}
		}
	} else {
		t.Error("gen_ai.system_instructions not found")
	}

	// Verify gen_ai.tool.definitions is ArrayValue of KvlistValues
	if toolDefs := attrMap["gen_ai.tool.definitions"]; toolDefs != nil {
		arr := toolDefs.GetArrayValue()
		if arr == nil {
			t.Errorf("gen_ai.tool.definitions: expected ArrayValue, got %T", toolDefs.Value)
		} else {
			for i, tool := range arr.Values {
				kvlist := tool.GetKvlistValue()
				if kvlist == nil {
					t.Errorf("gen_ai.tool.definitions[%d]: expected KvlistValue, got %T", i, tool.Value)
				} else {
					// Verify it has expected keys
					for _, key := range []string{"type", "name", "description"} {
						if findInKvlist(kvlist.Values, key) == nil {
							t.Errorf("gen_ai.tool.definitions[%d]: expected key '%s'", i, key)
						}
					}
				}
			}
		}
	} else {
		t.Error("gen_ai.tool.definitions not found")
	}
}

func TestOTelHelperFunctions(t *testing.T) {
	// Test otelString
	t.Run("otelString", func(t *testing.T) {
		av := otelString("test")
		if av.GetStringValue() != "test" {
			t.Errorf("otelString: expected 'test', got %v", av.GetStringValue())
		}
	})

	// Test otelArray
	t.Run("otelArray", func(t *testing.T) {
		av := otelArray(otelString("a"), otelString("b"))
		arr := av.GetArrayValue()
		if arr == nil || len(arr.Values) != 2 {
			t.Errorf("otelArray: expected array of 2 elements")
		}
	})

	// Test otelKVList
	t.Run("otelKVList", func(t *testing.T) {
		av := otelKVList(
			otelKV("key1", otelString("val1")),
			otelKV("key2", otelString("val2")),
		)
		kvlist := av.GetKvlistValue()
		if kvlist == nil || len(kvlist.Values) != 2 {
			t.Errorf("otelKVList: expected kvlist with 2 entries")
		}
	})

	// Test otelKV
	t.Run("otelKV", func(t *testing.T) {
		kv := otelKV("mykey", otelString("myval"))
		if kv.Key != "mykey" {
			t.Errorf("otelKV: expected key='mykey', got '%s'", kv.Key)
		}
		if kv.Value.GetStringValue() != "myval" {
			t.Errorf("otelKV: expected value='myval'")
		}
	})
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
