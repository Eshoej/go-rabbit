package gorabbit

import (
	"context"
	"testing"
	"time"
)

// TestDefaultConfiguration verifies that default configuration returns expected values.
func TestDefaultConfiguration(t *testing.T) {
	config := DefaultConfiguration()
	if config.ServiceName != "defaultService" {
		t.Errorf("Expected ServiceName 'defaultService', got '%s'", config.ServiceName)
	}
	if config.Connection.Host != "localhost" {
		t.Errorf("Expected Host 'localhost', got '%s'", config.Connection.Host)
	}
}

// TestNewGoRabbit verifies instantiation.
func TestNewGoRabbit(t *testing.T) {
	config := DefaultConfiguration()
	config.ServiceName = "test-service"
	// Use nil logger for testing
	rabbit, err := NewGoRabbit(config, nil)
	if err != nil {
		t.Fatalf("Failed to create NewGoRabbit: %v", err)
	}
	if rabbit == nil {
		t.Fatal("Expected rabbit instance, got nil")
	}
}

// Ensure interface compliance (if we had exported an interface, which we haven't yet, but good practice)
// func TestInterfaceCompliance(t *testing.T) {
// 	 var _ RabbitClient = (*GoRabbit)(nil)
// }

// Integration tests (skipped if no RabbitMQ available)
// Ideally, we would checking for an env var like TEST_INTEGRATION=1

func TestConnection_SkippedWithoutUnmockedEnvironment(t *testing.T) {
	// This serves as a placeholder for real integration tests.
	// In a real scenario, we might spin up a docker container.
	t.Skip("Skipping integration test requiring running RabbitMQ")

	config := DefaultConfiguration()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_ = ctx // usage

	rabbit, _ := NewGoRabbit(config, nil)
	err := rabbit.CheckConnection()
	if err == nil {
		// If we happen to have one running, great.
		t.Log("Combined luck: RabbitMQ is running locally!")
	} else {
		t.Logf("RabbitMQ not reachable: %v", err)
	}
}
