// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azureeventhubreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/azureeventhubreceiver"

import (
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/azure"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/translator/azurelogs"
)

type logsUnmarshaler interface {
	UnmarshalLogs([]byte) (plog.Logs, error)
}

type azureResourceLogsEventUnmarshaler[T any] struct {
	unmarshaler logsUnmarshaler
}

func newAzureResourceLogsUnmarshaler(buildInfo component.BuildInfo, logger *zap.Logger, applySemanticConventions bool, timeFormat []string) eventLogsUnmarshaler[*eventhub.Event] {
	if applySemanticConventions {
		return &azureResourceLogsEventUnmarshaler[*eventhub.Event]{
			unmarshaler: &azurelogs.ResourceLogsUnmarshaler{
				Version:     buildInfo.Version,
				Logger:      logger,
				TimeFormats: timeFormat,
			},
		}
	}
	return &azureResourceLogsEventUnmarshaler[*eventhub.Event]{
		unmarshaler: &azure.ResourceLogsUnmarshaler{
			Version:     buildInfo.Version,
			Logger:      logger,
			TimeFormats: timeFormat,
		},
	}
}

// UnmarshalLogs takes a byte array containing a JSON-encoded
// payload with Azure log records and transforms it into
// an OpenTelemetry plog.Logs object. The data in the Azure
// log record appears as fields and attributes in the
// OpenTelemetry representation; the bodies of the
// OpenTelemetry log records are empty.
func (r *azureResourceLogsEventUnmarshaler[T]) UnmarshalLogs(event T) (plog.Logs, error) {
	// Type assertion to handle the specific case for *eventhub.Event
	if eventhubEvent, ok := any(event).(*eventhub.Event); ok {
		return r.unmarshaler.UnmarshalLogs(eventhubEvent.Data)
	}

	// Return empty logs for unsupported types
	return plog.NewLogs(), nil
}
