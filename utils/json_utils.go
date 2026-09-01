package utils

import (
	"encoding/json"
	"fmt"
)

// ToJSON 将对象转换为JSON字符串
func ToJSON(obj interface{}) (string, error) {
	if obj == nil {
		return "", nil
	}

	bytes, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("failed to marshal object to JSON: %w", err)
	}

	return string(bytes), nil
}

// FromJSON 从JSON字符串解析对象
func FromJSON(jsonStr string, obj interface{}) error {
	if jsonStr == "" {
		return nil
	}

	if err := json.Unmarshal([]byte(jsonStr), obj); err != nil {
		return fmt.Errorf("failed to unmarshal JSON to object: %w", err)
	}

	return nil
}
