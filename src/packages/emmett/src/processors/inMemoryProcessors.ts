import { getInMemoryDatabase, type InMemoryDatabase } from '../database';
import { EmmettError } from '../errors';
import {
  type AnyEvent,
  type AnyMessage,
  type BatchRecordedMessageHandlerWithContext,
  type GlobalPositionTypeOfRecordedMessageMetadata,
  type MessageHandlerResult,
  type ReadEventMetadataWithGlobalPosition,
  type SingleRecordedMessageHandlerWithContext,
} from '../typing';
import {
  getCheckpoint,
  MessageProcessor,
  projector,
  reactor,
  type Checkpointer,
  type MessageProcessingScope,
  type ProjectorOptions,
  type ReactorOptions,
} from './processors';

export type InMemoryProcessorHandlerContext = {
  database: InMemoryDatabase;
};

export type InMemoryProcessor<
  MessageType extends AnyMessage = AnyMessage,
  // An in-memory processor can be driven by another store's consumer, which checkpoints
  // in its own format, so the type is a parameter rather than this store's default.
  CheckpointType =
    GlobalPositionTypeOfRecordedMessageMetadata<ReadEventMetadataWithGlobalPosition>,
> = MessageProcessor<
  MessageType,
  // TODO: generalize this to support other metadata types
  ReadEventMetadataWithGlobalPosition,
  InMemoryProcessorHandlerContext,
  CheckpointType
> & { database: InMemoryDatabase };

export type InMemoryProcessorEachMessageHandler<
  MessageType extends AnyMessage = AnyMessage,
> = SingleRecordedMessageHandlerWithContext<
  MessageType,
  ReadEventMetadataWithGlobalPosition,
  InMemoryProcessorHandlerContext
>;

export type InMemoryProcessorEachBatchHandler<
  MessageType extends AnyMessage = AnyMessage,
> = BatchRecordedMessageHandlerWithContext<
  MessageType,
  ReadEventMetadataWithGlobalPosition,
  InMemoryProcessorHandlerContext
>;

export type InMemoryProcessorConnectionOptions = {
  database?: InMemoryDatabase;
};

type CheckpointDocument<CheckpointType> = {
  _id: string;
  lastCheckpoint: CheckpointType | null;
};

export type InMemoryCheckpointer<
  MessageType extends AnyMessage = AnyMessage,
  CheckpointType =
    GlobalPositionTypeOfRecordedMessageMetadata<ReadEventMetadataWithGlobalPosition>,
> = Checkpointer<
  MessageType,
  ReadEventMetadataWithGlobalPosition,
  InMemoryProcessorHandlerContext,
  CheckpointType
>;

export const inMemoryCheckpointer = <
  MessageType extends AnyMessage = AnyMessage,
  CheckpointType =
    GlobalPositionTypeOfRecordedMessageMetadata<ReadEventMetadataWithGlobalPosition>,
>(): InMemoryCheckpointer<MessageType, CheckpointType> => {
  return {
    read: async ({ processorId }, { database }) => {
      const checkpoint = await database
        .collection<
          CheckpointDocument<CheckpointType>
        >('emt_processor_checkpoints')
        .findOne((d) => d._id === processorId);

      return Promise.resolve({
        lastCheckpoint: checkpoint?.lastCheckpoint ?? null,
      });
    },
    store: async (context, { database }) => {
      const { message, processorId, lastCheckpoint } = context;
      const checkpoints = database.collection<
        CheckpointDocument<CheckpointType>
      >('emt_processor_checkpoints');

      const checkpoint = await checkpoints.findOne(
        (d) => d._id === processorId,
      );

      const currentPosition = checkpoint?.lastCheckpoint ?? null;

      const newCheckpoint: CheckpointType | null = getCheckpoint(message);

      if (
        currentPosition &&
        (currentPosition === newCheckpoint ||
          currentPosition !== lastCheckpoint)
      ) {
        return {
          success: false,
          reason:
            currentPosition === newCheckpoint
              ? 'IGNORED'
              : newCheckpoint !== null &&
                  newCheckpoint !== undefined &&
                  currentPosition > newCheckpoint
                ? 'CURRENT_AHEAD'
                : 'MISMATCH',
        };
      }

      await checkpoints.handle(processorId, (existing) => ({
        ...(existing ?? {}),
        _id: processorId,
        lastCheckpoint: newCheckpoint,
      }));

      return { success: true, newCheckpoint };
    },
  };
};

type InMemoryConnectionOptions = {
  connectionOptions?: InMemoryProcessorConnectionOptions;
};

export type InMemoryReactorOptions<
  MessageType extends AnyMessage = AnyMessage,
  CheckpointType =
    GlobalPositionTypeOfRecordedMessageMetadata<ReadEventMetadataWithGlobalPosition>,
> = ReactorOptions<
  MessageType,
  ReadEventMetadataWithGlobalPosition,
  InMemoryProcessorHandlerContext,
  CheckpointType
> &
  InMemoryConnectionOptions;

export type InMemoryProjectorOptions<
  EventType extends AnyEvent = AnyEvent,
  CheckpointType =
    GlobalPositionTypeOfRecordedMessageMetadata<ReadEventMetadataWithGlobalPosition>,
> = ProjectorOptions<
  EventType,
  ReadEventMetadataWithGlobalPosition,
  InMemoryProcessorHandlerContext,
  CheckpointType
> &
  InMemoryConnectionOptions;

export type InMemoryProcessorOptions<
  MessageType extends AnyMessage = AnyMessage,
> =
  | InMemoryReactorOptions<MessageType>
  | InMemoryProjectorOptions<MessageType & AnyEvent>;

const inMemoryProcessingScope = (options: {
  database: InMemoryDatabase | null;
  processorId: string;
}): MessageProcessingScope<InMemoryProcessorHandlerContext> => {
  const processorDatabase = options.database;

  const processingScope: MessageProcessingScope<
    InMemoryProcessorHandlerContext
  > = <Result = MessageHandlerResult>(
    handler: (
      context: InMemoryProcessorHandlerContext,
    ) => Result | Promise<Result>,
    partialContext: Partial<InMemoryProcessorHandlerContext>,
  ) => {
    const database = processorDatabase ?? partialContext?.database;

    if (!database)
      throw new EmmettError(
        `InMemory processor '${options.processorId}' is missing database. Ensure that you passed it through options`,
      );

    return handler({ ...partialContext, database });
  };

  return processingScope;
};

export const inMemoryProjector = <
  EventType extends AnyEvent = AnyEvent,
  CheckpointType =
    GlobalPositionTypeOfRecordedMessageMetadata<ReadEventMetadataWithGlobalPosition>,
>(
  options: InMemoryProjectorOptions<EventType, CheckpointType>,
): InMemoryProcessor<EventType, CheckpointType> => {
  const database = options.connectionOptions?.database ?? getInMemoryDatabase();

  const hooks = {
    onInit: options.hooks?.onInit,
    onStart: options.hooks?.onStart,
    onClose: options.hooks?.onClose
      ? async (context: InMemoryProcessorHandlerContext) => {
          if (options.hooks?.onClose) await options.hooks?.onClose(context);
        }
      : undefined,
  };

  const processor = projector<
    EventType,
    ReadEventMetadataWithGlobalPosition,
    InMemoryProcessorHandlerContext,
    CheckpointType
  >({
    ...options,
    hooks,
    processingScope: inMemoryProcessingScope({
      database,
      processorId:
        options.processorId ?? `projection:${options.projection.name}`,
    }),
    checkpoints: inMemoryCheckpointer<EventType, CheckpointType>(),
  });

  return Object.assign(processor, { database });
};

export const inMemoryReactor = <
  MessageType extends AnyMessage = AnyMessage,
  CheckpointType =
    GlobalPositionTypeOfRecordedMessageMetadata<ReadEventMetadataWithGlobalPosition>,
>(
  options: InMemoryReactorOptions<MessageType, CheckpointType>,
): InMemoryProcessor<MessageType, CheckpointType> => {
  const database = options.connectionOptions?.database ?? getInMemoryDatabase();

  const hooks = {
    onInit: options.hooks?.onInit,
    onStart: options.hooks?.onStart,
    onClose: options.hooks?.onClose,
  };

  const processor = reactor<
    MessageType,
    ReadEventMetadataWithGlobalPosition,
    InMemoryProcessorHandlerContext,
    CheckpointType
  >({
    ...options,
    hooks,
    processingScope: inMemoryProcessingScope({
      database,
      processorId: options.processorId,
    }),
    checkpoints: inMemoryCheckpointer<MessageType, CheckpointType>(),
  });

  return Object.assign(processor, { database });
};
