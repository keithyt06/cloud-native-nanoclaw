// Web Widget -- Message Handler
// Handles WebSocket JSON messages for the web-widget channel.
// Three actions: sendMessage (with optional attachments), requestUploadUrl, getHistory.
//
// Pattern: follows dingtalk/message-handler.ts for DynamoDB storage and SQS FIFO dispatch,
// and web/gateway-manager.ts for WebSocket message format and ACK pattern.

import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import type pino from 'pino';
import type { WebSocket } from 'ws';
import type { Attachment, Message, SqsInboundPayload } from '@clawbot/shared';
import { config } from '../config.js';
import {
  putMessage,
  getOrCreateGroup,
  listGroups,
  getUser,
  getRecentMessages,
} from '../services/dynamo.js';
import { getCachedBot } from '../services/cached-lookups.js';
import path from 'node:path';
import type { WebWidgetConnection } from './gateway-manager.js';

// ── Types ───────────────────────────────────────────────────────────────────

/** Inbound WebSocket message from a web-widget client. */
export interface WebWidgetInboundMessage {
  action: 'sendMessage' | 'requestUploadUrl' | 'getHistory';
  text?: string;
  attachments?: WebWidgetRawAttachment[];
  fileName?: string;
  mimeType?: string;
  size?: number;
  limit?: number;
}

/** Raw attachment in a sendMessage payload. */
export interface WebWidgetRawAttachment {
  fileName: string;
  mimeType: string;
  size: number;
  /** Base64-encoded file data (for files <= 256 KB). */
  data?: string;
  /** S3 key from a completed presigned upload. */
  s3Key?: string;
}

/** Injected dependencies for message handler functions. */
export interface MessageHandlerDeps {
  logger: pino.Logger;
  s3: S3Client;
  sqs: SQSClient;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/** Send a JSON message over a WebSocket, swallowing errors on closed sockets. */
function safeSend(ws: WebSocket, data: unknown): void {
  try {
    if (ws.readyState === 1 /* OPEN */) {
      ws.send(JSON.stringify(data));
    }
  } catch {
    // Swallow — caller logs if needed
  }
}

/**
 * Infer the attachment type category from a MIME type string.
 */
export function inferAttachmentType(
  mimeType: string,
): 'image' | 'audio' | 'document' | 'video' {
  if (mimeType.startsWith('image/')) return 'image';
  if (mimeType.startsWith('audio/')) return 'audio';
  if (mimeType.startsWith('video/')) return 'video';
  return 'document';
}

/**
 * Sanitize a file name to prevent path traversal and special character injection.
 * Strips directory components and replaces unsafe characters with underscores.
 */
function sanitizeFileName(name: string): string {
  return path.basename(name).replace(/[^\w.\-]/g, '_') || 'file';
}

// ── handleSendMessage ───────────────────────────────────────────────────────

/**
 * Process a sendMessage action from a web-widget client.
 *
 * 1. Generate messageId
 * 2. Parse attachments (base64 -> S3 / s3Key direct reference)
 * 3. Quota check + ensure group exists
 * 4. Store message in DynamoDB (TTL 90 days)
 * 5. Send ACK to client
 * 6. Enqueue to SQS FIFO for agent dispatch
 */
export async function handleSendMessage(
  connId: string,
  conn: WebWidgetConnection,
  parsed: WebWidgetInboundMessage,
  deps: MessageHandlerDeps,
): Promise<void> {
  const { logger } = deps;
  const text = parsed.text || '';

  if (!text.trim() && (!parsed.attachments || parsed.attachments.length === 0)) {
    safeSend(conn.ws, { type: 'error', message: 'Empty message' });
    return;
  }

  const { botId, groupJid, userId, userName, channelId, ownerUserId } = conn;

  // Generate message ID
  const messageId = `ww-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

  // Load bot config
  const bot = await getCachedBot(botId);
  if (!bot || bot.status !== 'active') {
    safeSend(conn.ws, { type: 'error', message: 'Bot is not active' });
    return;
  }

  // ── Attachment processing ───────────────────────────────────────────────
  const attachments: Attachment[] = [];

  if (parsed.attachments && parsed.attachments.length > 0) {
    for (const raw of parsed.attachments) {
      try {
        let s3Key: string;

        if (raw.data) {
          // Base64 inline upload (small files <= 256 KB)
          const buffer = Buffer.from(raw.data, 'base64');
          s3Key = `${ownerUserId}/${botId}/attachments/${messageId}/${sanitizeFileName(raw.fileName)}`;
          await deps.s3.send(
            new PutObjectCommand({
              Bucket: config.s3Bucket,
              Key: s3Key,
              Body: buffer,
              ContentType: raw.mimeType,
            }),
          );
        } else if (raw.s3Key) {
          // Validate the s3Key belongs to this user's bot path (prevent cross-tenant access)
          const expectedPrefix = `${ownerUserId}/${botId}/attachments/`;
          if (!raw.s3Key.startsWith(expectedPrefix)) {
            logger.warn({ connId, s3Key: raw.s3Key, expectedPrefix }, 'Rejected s3Key: path does not match expected prefix');
            continue;
          }
          s3Key = raw.s3Key;
        } else {
          logger.warn({ connId, fileName: raw.fileName }, 'Attachment has neither data nor s3Key, skipping');
          continue;
        }

        attachments.push({
          type: inferAttachmentType(raw.mimeType),
          s3Key,
          mimeType: raw.mimeType,
          fileName: raw.fileName,
          size: raw.size,
        });
      } catch (err) {
        logger.warn(
          { err, connId, fileName: raw.fileName },
          'Failed to process web-widget attachment',
        );
      }
    }
  }

  // ── Quota check + group creation ────────────────────────────────────────
  const existingGroups = await listGroups(botId);
  const isNewGroup = !existingGroups.find((g) => g.groupJid === groupJid);
  if (isNewGroup) {
    const owner = await getUser(bot.userId);
    const maxGroups = owner?.quota?.maxGroupsPerBot ?? 10;
    if (existingGroups.length >= maxGroups) {
      safeSend(conn.ws, { type: 'error', message: 'Group limit reached' });
      return;
    }
  }

  await getOrCreateGroup(botId, groupJid, `web-widget-${channelId}-${userId}`, 'web-widget', false);

  // ── Annotate content with attachment info ───────────────────────────────
  let annotatedContent = text;
  if (attachments.length > 0) {
    const fileDescs = attachments
      .map((a) => `- ${a.fileName || a.s3Key.split('/').pop()} (${a.mimeType})`)
      .join('\n');
    annotatedContent += `\n[Attached files — saved to /workspace/group/attachments/]\n${fileDescs}`;
  }

  // ── Store message in DynamoDB ───────────────────────────────────────────
  const timestamp = new Date().toISOString();
  const msg: Message = {
    botId,
    groupJid,
    timestamp,
    messageId,
    sender: userId,
    senderName: userName,
    content: annotatedContent,
    isFromMe: false,
    isBotMessage: false,
    channelType: 'web-widget',
    ttl: Math.floor(Date.now() / 1000) + 90 * 24 * 3600,
    ...(attachments.length > 0 && { attachments }),
  };

  try {
    await putMessage(msg);
  } catch (err) {
    logger.error(
      { err, botId, messageId, groupJid },
      'Failed to store web-widget message in DynamoDB',
    );
    throw err;
  }

  // ── Send ACK to client ─────────────────────────────────────────────────
  safeSend(conn.ws, { type: 'ack', messageId });

  logger.info(
    {
      botId,
      groupJid,
      messageId,
      contentLength: text.length,
      hasAttachments: attachments.length > 0,
    },
    'Web-widget message received, dispatching to SQS',
  );

  // ── SQS FIFO dispatch ──────────────────────────────────────────────────
  const sqsPayload: SqsInboundPayload = {
    type: 'inbound_message',
    botId,
    groupJid,
    userId: bot.userId,
    messageId,
    content: msg.content,
    channelType: 'web-widget',
    timestamp,
    ...(attachments.length > 0 && { attachments }),
    replyContext: {
      webWidgetChannelId: channelId,
      webWidgetUserId: userId,
    },
  };

  try {
    await deps.sqs.send(
      new SendMessageCommand({
        QueueUrl: config.queues.messages,
        MessageBody: JSON.stringify(sqsPayload),
        MessageGroupId: `${botId}#${groupJid}`,
        MessageDeduplicationId: messageId,
      }),
    );
  } catch (err) {
    logger.error(
      { err, botId, messageId, groupJid, queueUrl: config.queues.messages },
      'Failed to dispatch web-widget message to SQS — message stored but not queued',
    );
    throw err;
  }

  logger.info({ botId, groupJid, messageId }, 'Web-widget message dispatched to SQS');
}

// ── handleRequestUploadUrl ──────────────────────────────────────────────────

/**
 * Generate a presigned S3 PUT URL so the client can upload a large file directly.
 *
 * 1. Generate s3Key
 * 2. Create presigned PutObject URL (5 min expiry)
 * 3. Send { type: 'uploadUrl', uploadUrl, s3Key, expiresIn } to client
 */
export async function handleRequestUploadUrl(
  connId: string,
  conn: WebWidgetConnection,
  parsed: WebWidgetInboundMessage,
  deps: MessageHandlerDeps,
): Promise<void> {
  const { logger } = deps;
  const { botId, ownerUserId } = conn;
  const fileName = parsed.fileName || `upload-${Date.now()}`;
  const mimeType = parsed.mimeType || 'application/octet-stream';

  const s3Key = `${ownerUserId}/${botId}/attachments/${Date.now()}-${sanitizeFileName(fileName)}`;

  try {
    const uploadUrl = await getSignedUrl(
      deps.s3,
      new PutObjectCommand({
        Bucket: config.s3Bucket,
        Key: s3Key,
        ContentType: mimeType,
      }),
      { expiresIn: 300 },
    );

    safeSend(conn.ws, {
      type: 'uploadUrl',
      uploadUrl,
      s3Key,
      expiresIn: 300,
    });

    logger.debug({ connId, botId, s3Key }, 'Web-widget presigned upload URL generated');
  } catch (err) {
    logger.error({ err, connId, botId }, 'Failed to generate presigned upload URL');
    safeSend(conn.ws, { type: 'error', message: 'Failed to generate upload URL' });
  }
}

// ── handleGetHistory ────────────────────────────────────────────────────────

/**
 * Retrieve recent messages for the connection's group and send them to the client.
 *
 * 1. Query DynamoDB messages table (botId#groupJid, limit default 50)
 * 2. Send { type: 'history', messages: [...] } to client
 */
export async function handleGetHistory(
  conn: WebWidgetConnection,
  limit: number | undefined,
  deps: MessageHandlerDeps,
): Promise<void> {
  const { logger } = deps;

  try {
    const messages = await getRecentMessages(conn.botId, conn.groupJid, limit || 50);
    safeSend(conn.ws, {
      type: 'history',
      messages: messages.map((m) => ({
        messageId: m.messageId,
        text: m.content,
        sender: m.isFromMe ? 'bot' : 'user',
        senderName: m.senderName,
        timestamp: m.timestamp,
        ...(m.attachments && m.attachments.length > 0 && {
          attachments: m.attachments.map((a) => ({
            type: a.type,
            fileName: a.fileName,
            mimeType: a.mimeType,
            size: a.size,
          })),
        }),
      })),
    });
  } catch (err) {
    logger.error(
      { err, botId: conn.botId, groupJid: conn.groupJid },
      'Failed to load web-widget history',
    );
    safeSend(conn.ws, { type: 'error', message: 'Failed to load history' });
  }
}
