// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azureeventhubreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/azureeventhubreceiver"

import (
	"time"

	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type rawLogsUnmarshaler[T any] struct {
	logger *zap.Logger
}

func newRawLogsUnmarshaler(logger *zap.Logger) eventLogsUnmarshaler[*eventhub.Event] {
	return &rawLogsUnmarshaler[*eventhub.Event]{
		logger: logger,
	}
}

func (r *rawLogsUnmarshaler[T]) UnmarshalLogs(event T) (plog.Logs, error) {
	// Type assertion to handle the specific case for *eventhub.Event
	if eventhubEvent, ok := any(event).(*eventhub.Event); ok {
		l := plog.NewLogs()
		lr := l.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		slice := lr.Body().SetEmptyBytes()
		slice.Append(eventhubEvent.Data...)
		lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		if eventhubEvent.SystemProperties.EnqueuedTime != nil {
			lr.SetTimestamp(pcommon.NewTimestampFromTime(*eventhubEvent.SystemProperties.EnqueuedTime))
		}

		if err := lr.Attributes().FromRaw(eventhubEvent.Properties); err != nil {
			return l, err
		}

		return l, nil
	}
	
	// Return empty logs for unsupported types
	return plog.NewLogs(), nil
}
