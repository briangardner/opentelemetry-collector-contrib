// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azureeventhubreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/featuregate"
)

func TestFeatureGateRegistration(t *testing.T) {
	// Test that the feature gate is properly registered
	// Since we can't directly look up in the registry, we test that our exported variable is valid
	require.NotNil(t, useAzEventHubsFeatureGate, "Feature gate should be properly initialized")

	// Test that the feature gate has a valid ID
	assert.NotEmpty(t, useAzEventHubsFeatureGate.ID(), "Feature gate should have a valid ID")
}

func TestFeatureGateProperties(t *testing.T) {
	gate := useAzEventHubsFeatureGate

	// Test feature gate name
	assert.Equal(t, featureGateName, gate.ID(), "Feature gate should have correct name")

	// Test feature gate stage
	assert.Equal(t, featuregate.StageAlpha, gate.Stage(), "Feature gate should be in Alpha stage")

	// Test feature gate description
	expectedDescription := "When enabled, the Azure Event Hub receiver will use the new `azeventhubs` client to consume messages."
	assert.Equal(t, expectedDescription, gate.Description(), "Feature gate should have correct description")
}

func TestFeatureGateNameConstant(t *testing.T) {
	// Test that the constant is properly defined
	assert.NotEmpty(t, featureGateName, "Feature gate name constant should not be empty")
	assert.Equal(t, "receiver.azureeventhubreceiver.UseAzEventHubs", featureGateName, "Feature gate name should match expected value")
}

func TestFeatureGateInitialState(t *testing.T) {
	gate := useAzEventHubsFeatureGate

	// Test initial state (should be disabled by default for Alpha stage)
	assert.False(t, gate.IsEnabled(), "Alpha stage feature gate should be disabled by default")
}
