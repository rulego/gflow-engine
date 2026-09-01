package config

import "testing"

func TestConfig_Validate_NilDatabase(t *testing.T) {
	c := &Config{Database: nil}
	if err := c.Validate(); err == nil {
		t.Error("nil Database should fail validation")
	}
}

func TestConfig_Validate_EmptyDriver(t *testing.T) {
	c := &Config{Database: &DatabaseConfig{Driver: "", Dsn: "dsn"}}
	if err := c.Validate(); err == nil {
		t.Error("empty Driver should fail validation")
	}
}

func TestConfig_Validate_EmptyDSN(t *testing.T) {
	c := &Config{Database: &DatabaseConfig{Driver: "postgres", Dsn: ""}}
	if err := c.Validate(); err == nil {
		t.Error("empty Dsn should fail validation")
	}
}

func TestConfig_Validate_Valid(t *testing.T) {
	c := &Config{Database: &DatabaseConfig{Driver: "postgres", Dsn: "host=localhost"}}
	if err := c.Validate(); err != nil {
		t.Errorf("valid config should pass: %v", err)
	}
}
