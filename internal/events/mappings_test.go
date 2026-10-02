package events

import (
	"strings"
	"testing"
)

func TestNativeMappingValidation(t *testing.T) {
	cfg := eventConfig(t)
	scheduled := `mappings:
 - rule_arn: arn:aws:events:ap-south-1:123456789012:rule/bse
   event_type: daily-bhavcopy
   inputs: {exchangeName: BSE}
`
	native := `mappings:
 - account: '123456789012'
   region: ap-south-1
   source: bond-producer
   detail_type: ISIN
   event_type: nsdl-bond-data
   detail_fields: {isin_code: isin}
`
	for _, valid := range []string{scheduled, native, "mappings: []"} {
		if _, err := LoadMappings([]byte(valid), cfg); err != nil {
			t.Fatal(err)
		}
	}
	invalid := []string{"[", "unknown: true", scheduled + "---\nmappings: []", strings.ReplaceAll(scheduled, "daily-bhavcopy", "unknown"), strings.ReplaceAll(scheduled, "arn:aws:events:ap-south-1:123456789012:rule/bse", "bad"), strings.ReplaceAll(native, "region: ap-south-1", "region: ''"), scheduled + strings.TrimPrefix(scheduled, "mappings:\n"), strings.ReplaceAll(scheduled, "exchangeName: BSE", "extra: BSE"), strings.ReplaceAll(scheduled, "exchangeName: BSE", "exchangeName: 3"), strings.ReplaceAll(native, "isin_code: isin", "extra: isin"), native + "   inputs: {isin_code: INE121A07QY9}\n", strings.ReplaceAll(scheduled, "inputs: {exchangeName: BSE}", "inputs: {}"), strings.ReplaceAll(scheduled, "exchangeName: BSE", "exchangeName: ../bad")}
	for _, raw := range invalid {
		if _, err := LoadMappings([]byte(raw), cfg); err == nil {
			t.Fatalf("accepted invalid mappings %s", raw)
		}
	}
}
