package absurd

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

func validateQueueName(queueName string) (string, error) {
	if queueName == "" {
		return "", fmt.Errorf("queue name must be provided")
	}
	if len([]byte(queueName)) > maxQueueNameLength {
		return "", fmt.Errorf("queue name %q is too long (max %d bytes)", queueName, maxQueueNameLength)
	}
	return queueName, nil
}

func marshalJSON(value any) (json.RawMessage, error) {
	if value == nil {
		return json.RawMessage("null"), nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func unmarshalJSON(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	return json.Unmarshal(raw, dst)
}

func normalizeRawJSON(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("null")
	}
	return cloneRawJSON(raw)
}

func cloneRawJSON(raw []byte) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	cloned := make([]byte, len(raw))
	copy(cloned, raw)
	return json.RawMessage(cloned)
}

func cloneMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func durationSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(math.Ceil(d.Seconds()))
}

func durationSecondsOrDefault(d time.Duration, fallback time.Duration) int {
	if d <= 0 {
		d = fallback
	}
	return durationSeconds(d)
}
