package azureeventhubreceiver

import (
	"context"
	"errors"

	azeventhubs "github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/xextension/storage"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/stanza/adapter"
)

type azEventHubWrapperImpl struct {
	client *azeventhubs.ConsumerClient
}

func (h *azEventHubWrapperImpl) GetRuntimeInformation(ctx context.Context) (azeventhubs.EventHubProperties, error) {
	return h.client.GetEventHubProperties(ctx, nil)
}

func (h *azEventHubWrapperImpl) Receive(ctx context.Context, partitionID string, handler azeventhubs.Handler, opts ...azeventhubs.ReceiveOption) (listerHandleWrapper, error) {
	//l, err := h.hub.Receive(ctx, partitionID, handler, opts...)
	//return l, err
	//TODO: Implement this
	return nil, nil
}

func (h *azEventHubWrapperImpl) Close(ctx context.Context) error {
	return h.client.Close(ctx)
}

type azEventHubHandler struct {
	hub           hubWrapper
	dataConsumer  dataConsumer[*azeventhubs.EventData]
	config        *Config
	settings      receiver.Settings
	cancel        context.CancelFunc
	storageClient storage.Client
}

func (h *azEventHubHandler) run(ctx context.Context, host component.Host) error {
	ctx, h.cancel = context.WithCancel(ctx)

	if h.storageClient == nil { // set manually for testing.
		storageClient, err := adapter.GetStorageClient(ctx, host, h.config.StorageID, h.settings.ID)
		if err != nil {
			h.settings.Logger.Debug("Error connecting to Storage", zap.Error(err))
			return err
		}
		h.storageClient = storageClient
	}

	if h.hub == nil { // set manually for testing.
		hub, newHubErr := azeventhubs.NewConsumerClientFromConnectionString(h.config.Connection, h.config.ConsumerGroup, azeventhubs.ClientWithOffsetPersistence(&storageCheckpointPersister{storageClient: h.storageClient}))
		if newHubErr != nil {
			h.settings.Logger.Debug("Error connecting to Event Hub", zap.Error(newHubErr))
			return newHubErr
		}
		h.hub = &azEventHubWrapperImpl{client: hub}
	}

	if h.config.Partition != "" {
		err := h.setUpOnePartition(ctx, h.config.Partition, true)
		if err != nil {
			h.settings.Logger.Debug("Error setting up partition", zap.Error(err))
		}
		return err
	}

	// listen to each partition of the Event Hub
	runtimeInfo, err := h.hub.GetRuntimeInformation(ctx)
	if err != nil {
		h.settings.Logger.Debug("Error getting Runtime Information", zap.Error(err))
		return err
	}

	var errs []error
	for _, partitionID := range runtimeInfo.PartitionIDs {
		err = h.setUpOnePartition(ctx, partitionID, false)
		if err != nil {
			h.settings.Logger.Debug("Error setting up partition", zap.Error(err), zap.String("partition", partitionID))
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (h *azEventHubHandler) setUpOnePartition(ctx context.Context, partitionID string, applyOffset bool) error {
	receiverOptions := []azeventhubs.ReceiveOption{}
	if applyOffset && h.config.Offset != "" {
		receiverOptions = append(receiverOptions, azeventhubs.ReceiveWithStartingOffset(h.config.Offset))
	}

	if h.config.ConsumerGroup != "" {
		receiverOptions = append(receiverOptions, azeventhubs.ReceiveWithConsumerGroup(h.config.ConsumerGroup))
	}

	handle, err := h.hub.Receive(ctx, partitionID, h.newMessageHandler, receiverOptions...)
	if err != nil {
		return err
	}
	go func() {
		<-handle.Done()
		err := handle.Err()
		if err != nil {
			h.settings.Logger.Error("Error reported by event hub", zap.Error(err))
		}
	}()

	return nil
}

func (h *azEventHubHandler) newMessageHandler(ctx context.Context, event *azeventhubs.Event) error {
	err := h.dataConsumer.consume(ctx, event)
	if err != nil {
		h.settings.Logger.Error("error decoding message", zap.Error(err))
		return err
	}

	return nil
}

func (h *azEventHubHandler) close(ctx context.Context) error {
	var errs error
	if h.storageClient != nil {
		if err := h.storageClient.Close(ctx); err != nil {
			errs = errors.Join(errs, err)
		}
		h.storageClient = nil
	}

	if h.hub != nil {
		err := h.hub.Close(ctx)
		if err != nil {
			errs = errors.Join(errs, err)
		}
		h.hub = nil
	}
	if h.cancel != nil {
		h.cancel()
	}

	return errs
}

func (h *azEventHubHandler) setDataConsumer(dataConsumer dataConsumer[*azeventhubs.Event]) {
	h.dataConsumer = dataConsumer
}

func newAzEventHubHandler(config *Config, settings receiver.Settings) *azEventHubHandler {
	return &azEventHubHandler{
		config:   config,
		settings: settings,
	}
}
