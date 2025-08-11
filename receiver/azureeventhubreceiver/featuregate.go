package azureeventhubreceiver

import "go.opentelemetry.io/collector/featuregate"

const (
	featureGateName = "receiver.azureeventhubreceiver.UseAzEventHubs"
)

var useAzEventHubsFeatureGate = featuregate.GlobalRegistry().MustRegister(
	featureGateName, featuregate.StageAlpha,
	featuregate.WithRegisterDescription("When enabled, the Azure Event Hub receiver will use the new `azeventhubs` client to consume messages."),
)
