// ClawBot Cloud — Reply Queue Consumer
// Long-polls the SQS standard reply queue for agent replies
// Routes replies back to the originating channel

import {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
} from '@aws-sdk/client-sqs';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { config } from '../config.js';
import { getRegistry } from '../adapters/registry.js';
import { getWebWidgetGatewayManager } from '../web-widget/gateway-manager.js';
import type { ReplyContext } from '@clawbot/shared/channel-adapter';
import type { ChannelType, SqsReplyPayload } from '@clawbot/shared';
import type { Logger } from 'pino';

let running = false;

export function startReplyConsumer(logger: Logger): void {
  if (!config.queues.replies) {
    logger.warn('SQS_REPLIES_URL not set, reply consumer disabled');
    return;
  }
  running = true;
  replyLoop(logger).catch((err) =>
    logger.error(err, 'Reply consumer crashed'),
  );
}

export function stopReplyConsumer(): void {
  running = false;
}

async function replyLoop(logger: Logger): Promise<void> {
  const sqs = new SQSClient({ region: config.region });
  const s3 = new S3Client({ region: config.region });

  logger.info({ queueUrl: config.queues.replies }, 'Reply consumer started');

  while (running) {
    try {
      const result = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: config.queues.replies,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 20,
          VisibilityTimeout: 60,
        }),
      );

      if (!result.Messages || result.Messages.length === 0) {
        continue;
      }

      for (const msg of result.Messages) {
        try {
          const payload: SqsReplyPayload = JSON.parse(msg.Body!);

          // Web-widget replies must be handled by the leader instance (which
          // holds in-memory WebSocket connections). Other channel types use
          // external REST APIs and are unaffected by this check.
          if (payload.channelType === 'web-widget') {
            const widgetGw = getWebWidgetGatewayManager();
            if (!widgetGw || !widgetGw.isLeader()) {
              // Release message with a short delay (5s) to avoid tight bounce
              // loops when no leader exists yet. The leader will pick it up
              // once elected (typically within 15–30s).
              await sqs.send(
                new ChangeMessageVisibilityCommand({
                  QueueUrl: config.queues.replies,
                  ReceiptHandle: msg.ReceiptHandle!,
                  VisibilityTimeout: 5,
                }),
              );
              continue;
            }
          }

          // Route reply through adapter registry
          const registry = getRegistry();
          const adapter = registry.get(payload.channelType);

          if (!adapter) {
            logger.warn(
              { botId: payload.botId, channelType: payload.channelType },
              'No adapter registered for channel type',
            );
            // Delete the message anyway to avoid infinite retries
            await sqs.send(
              new DeleteMessageCommand({
                QueueUrl: config.queues.replies,
                ReceiptHandle: msg.ReceiptHandle!,
              }),
            );
            continue;
          }

          const ctx: ReplyContext = {
            botId: payload.botId,
            groupJid: payload.groupJid,
            channelType: payload.channelType as ChannelType,
          };

          if (payload.type === 'file_reply') {
            const resp = await s3.send(
              new GetObjectCommand({
                Bucket: config.s3Bucket,
                Key: payload.s3Key,
              }),
            );
            if (!resp.Body) {
              logger.error({ s3Key: payload.s3Key }, 'S3 file body is empty or missing, skipping');
              continue;
            }
            const fileBuffer = Buffer.from(
              await resp.Body.transformToByteArray(),
            );

            if (adapter.sendFile) {
              await adapter.sendFile(
                ctx,
                fileBuffer,
                payload.fileName,
                payload.mimeType,
                payload.caption,
              );
              logger.info(
                { botId: payload.botId, fileName: payload.fileName },
                'File sent via adapter',
              );
            } else {
              await adapter.sendReply(
                ctx,
                `[File: ${payload.fileName}] (file sending not supported on this channel)`,
              );
              logger.warn(
                { channelType: payload.channelType },
                'Adapter does not support sendFile, sent text fallback',
              );
            }
          } else {
            await adapter.sendReply(ctx, payload.text);
          }

          // Delete message on success
          await sqs.send(
            new DeleteMessageCommand({
              QueueUrl: config.queues.replies,
              ReceiptHandle: msg.ReceiptHandle!,
            }),
          );

          logger.info(
            {
              botId: payload.botId,
              groupJid: payload.groupJid,
              channelType: payload.channelType,
            },
            'Reply delivered via channel',
          );
        } catch (err) {
          logger.error(
            { err, messageId: msg.MessageId },
            'Failed to process reply message',
          );
          // Don't delete — let visibility timeout return it to queue for retry
        }
      }
    } catch (err) {
      logger.error(err, 'Reply consumer receive error');
      if (running) {
        await new Promise((resolve) => setTimeout(resolve, 5000));
      }
    }
  }

  logger.info('Reply consumer stopped');
}
