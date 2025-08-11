// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azureeventhubreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/azureeventhubreceiver"

import (
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/azure"
)

type azureTracesEventUnmarshaler[T any] struct {
	unmarshaler *azure.TracesUnmarshaler
}

func newAzureTracesUnmarshaler(buildInfo component.BuildInfo, logger *zap.Logger, timeFormat []string) eventTracesUnmarshaler[*eventhub.Event] {
	return &azureTracesEventUnmarshaler[*eventhub.Event]{
		unmarshaler: &azure.TracesUnmarshaler{
			Version:     buildInfo.Version,
			Logger:      logger,
			TimeFormats: timeFormat,
		},
	}
}

// UnmarshalTraces takes a byte array containing a JSON-encoded
// payload with Azure records and transforms it into
// an OpenTelemetry ptraces.traces object. The data in the Azure
// record appears as fields and attributes in the
// OpenTelemetry representation; the bodies of the
// OpenTelemetry trace records are empty.
func (r *azureTracesEventUnmarshaler[T]) UnmarshalTraces(event T) (ptrace.Traces, error) {
	// Type assertion to handle the specific case for *eventhub.Event
	if eventhubEvent, ok := any(event).(*eventhub.Event); ok {
		return r.unmarshaler.UnmarshalTraces(eventhubEvent.Data)
	}

	// Return empty traces for unsupported types
	return ptrace.NewTraces(), nil
}
