// Web Widget Channel Adapter
// Manages WebSocket-based web widget connections with leader election.
// Uses WebWidgetGatewayManager for inbound messages (WebSocket connections),
// pushes outbound replies directly to in-memory connections.
// Follows the same leader election pattern as WebAdapter.

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  PutCommand,
  DeleteCommand,
  GetCommand,
} from '@aws-sdk/lib-dynamodb';
import { S3Client, PutObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { BaseChannelAdapter } from '../base.js';
import type { ReplyContext, ReplyOptions } from '@clawbot/shared/channel-adapter';
import {
  type WebWidgetGatewayManager,
  initWebWidgetGatewayManager,
} from '../../web-widget/gateway-manager.js';
import { config } from '../../config.js';

// -- Leader Election Constants ------------------------------------------------

const LOCK_TABLE = config.tables.sessions;
const LOCK_PK = '__system__';
const LOCK_SK = 'web-widget-gateway-leader';
const LOCK_TTL_S = 30;
const RENEW_INTERVAL_MS = 15_000;
const POLL_INTERVAL_MS = 15_000;
const POLL_INITIAL_DELAY_MS = 5_000;

const INSTANCE_ID =
  process.env.ECS_TASK_ID ||
  `local-${process.pid}-${Date.now().toString(36)}`;

const ddb = DynamoDBDocumentClient.from(
  new DynamoDBClient({ region: config.region }),
);

const s3 = new S3Client({ region: config.region });

// -- Adapter ------------------------------------------------------------------

export class WebWidgetAdapter extends BaseChannelAdapter {
  readonly channelType = 'web-widget' as const;

  private isLeader = false;
  private gateway: WebWidgetGatewayManager | null = null;
  private renewTimer: ReturnType<typeof setInterval> | null = null;
  private pollTimer: ReturnType<typeof setInterval> | null = null;
  private initialPollTimer: ReturnType<typeof setTimeout> | null = null;
  private stopped = false;

  constructor(parentLogger: import('pino').Logger) {
    super(parentLogger);
    this.init();
  }

  // -- Lifecycle --------------------------------------------------------------

  async start(): Promise<void> {
    this.stopped = false;

    // Initialize the singleton gateway manager
    this.gateway = initWebWidgetGatewayManager(this.logger);

    const acquired = await this.tryAcquireLock();
    if (acquired) {
      await this.becomeLeader();
    } else {
      this.logger.info('WebWidget: another instance is leader, entering standby');
      this.startStandbyPoll();
    }
  }

  async stop(): Promise<void> {
    this.stopped = true;

    if (this.renewTimer) {
      clearInterval(this.renewTimer);
      this.renewTimer = null;
    }
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.initialPollTimer) {
      clearTimeout(this.initialPollTimer);
      this.initialPollTimer = null;
    }

    if (this.gateway) {
      this.gateway.setLeaderStatus(false);
      await this.gateway.stop();
    }

    if (this.isLeader) {
      await this.releaseLock();
      this.isLeader = false;
    }
  }

  /**
   * Expose gateway manager for index.ts WebSocket route registration.
   */
  getGateway(): WebWidgetGatewayManager | null {
    return this.gateway;
  }

  // -- Leader Election --------------------------------------------------------

  private async tryAcquireLock(): Promise<boolean> {
    const now = Math.floor(Date.now() / 1000);
    try {
      await ddb.send(
        new PutCommand({
          TableName: LOCK_TABLE,
          Item: {
            pk: LOCK_PK,
            sk: LOCK_SK,
            leaderId: INSTANCE_ID,
            expiresAt: now + LOCK_TTL_S,
          },
          ConditionExpression:
            'attribute_not_exists(pk) OR expiresAt < :now',
          ExpressionAttributeValues: { ':now': now },
        }),
      );
      this.logger.info({ instanceId: INSTANCE_ID }, 'WebWidget leader lock acquired');
      return true;
    } catch (err: unknown) {
      if ((err as { name?: string }).name === 'ConditionalCheckFailedException') {
        return false;
      }
      this.logger.error(err, 'Failed to acquire WebWidget leader lock');
      return false;
    }
  }

  private async renewLock(): Promise<boolean> {
    const now = Math.floor(Date.now() / 1000);
    try {
      await ddb.send(
        new PutCommand({
          TableName: LOCK_TABLE,
          Item: {
            pk: LOCK_PK,
            sk: LOCK_SK,
            leaderId: INSTANCE_ID,
            expiresAt: now + LOCK_TTL_S,
          },
          ConditionExpression: 'leaderId = :me',
          ExpressionAttributeValues: { ':me': INSTANCE_ID },
        }),
      );
      return true;
    } catch {
      this.logger.warn('Failed to renew WebWidget leader lock, stepping down');
      return false;
    }
  }

  private async releaseLock(): Promise<void> {
    try {
      await ddb.send(
        new DeleteCommand({
          TableName: LOCK_TABLE,
          Key: { pk: LOCK_PK, sk: LOCK_SK },
          ConditionExpression: 'leaderId = :me',
          ExpressionAttributeValues: { ':me': INSTANCE_ID },
        }),
      );
      this.logger.info('WebWidget leader lock released');
    } catch {
      // Already expired or taken
    }
  }

  private async isLockExpired(): Promise<boolean> {
    try {
      const res = await ddb.send(
        new GetCommand({
          TableName: LOCK_TABLE,
          Key: { pk: LOCK_PK, sk: LOCK_SK },
        }),
      );
      if (!res.Item) return true;
      return (res.Item.expiresAt as number) < Math.floor(Date.now() / 1000);
    } catch {
      return true;
    }
  }

  // -- Leader Lifecycle -------------------------------------------------------

  private async becomeLeader(): Promise<void> {
    this.isLeader = true;
    this.gateway!.setLeaderStatus(true);

    this.logger.info('WebWidget: became leader, starting gateway');

    try {
      await this.gateway!.start();
    } catch (err) {
      this.logger.error(err, 'Failed to start WebWidget gateway');
      this.isLeader = false;
      this.gateway!.setLeaderStatus(false);
      await this.releaseLock();
      return;
    }

    this.startRenewLoop();
  }

  private startRenewLoop(): void {
    this.renewTimer = setInterval(async () => {
      if (this.stopped) return;
      const ok = await this.renewLock();
      if (!ok) {
        this.logger.warn('Lost WebWidget leader lock, stopping gateway');
        this.isLeader = false;
        if (this.gateway) {
          this.gateway.setLeaderStatus(false);
          await this.gateway.stop();
        }
        if (!this.stopped) {
          this.startStandbyPoll();
        }
      }
    }, RENEW_INTERVAL_MS);
  }

  private startStandbyPoll(): void {
    const poll = async () => {
      if (this.stopped) return;
      const expired = await this.isLockExpired();
      if (expired) {
        this.logger.info('WebWidget leader lock expired, attempting takeover');
        if (this.pollTimer) {
          clearInterval(this.pollTimer);
          this.pollTimer = null;
        }
        const acquired = await this.tryAcquireLock();
        if (acquired) {
          await this.becomeLeader();
        } else {
          this.startStandbyPoll();
        }
      }
    };
    // First check quickly (covers rolling update where old leader just died)
    this.initialPollTimer = setTimeout(poll, POLL_INITIAL_DELAY_MS);
    // Then regular interval
    this.pollTimer = setInterval(poll, POLL_INTERVAL_MS);
  }

  // -- Send Reply -------------------------------------------------------------

  async sendReply(
    ctx: ReplyContext,
    text: string,
    _opts?: ReplyOptions,
  ): Promise<void> {
    try {
      if (!this.gateway) {
        this.logger.warn({ botId: ctx.botId }, 'WebWidget gateway not initialized');
        return;
      }

      const message = {
        type: 'message',
        text,
        sender: 'bot',
        timestamp: new Date().toISOString(),
      };

      // Try specific connection first (channelId + userId from replyContext)
      let sent = false;
      if (ctx.webWidgetChannelId && ctx.webWidgetUserId) {
        sent = this.gateway.pushToConnection(ctx.webWidgetChannelId, ctx.webWidgetUserId, message);
      }

      // Fall back to groupJid-based lookup
      if (!sent) {
        sent = this.gateway.pushToGroup(ctx.groupJid, message);
      }

      if (sent) {
        this.logger.info(
          { botId: ctx.botId, groupJid: ctx.groupJid },
          'WebWidget reply pushed to connection',
        );
      } else {
        this.logger.warn(
          { botId: ctx.botId, groupJid: ctx.groupJid },
          'No active web-widget connection found for reply (client may have disconnected)',
        );
      }
    } catch (err) {
      this.logger.error(
        { err, botId: ctx.botId, groupJid: ctx.groupJid },
        'Failed to send web-widget reply',
      );
    }
  }

  // -- Send File --------------------------------------------------------------

  async sendFile(
    ctx: ReplyContext,
    file: Buffer,
    fileName: string,
    mimeType: string,
    caption?: string,
  ): Promise<void> {
    try {
      if (!this.gateway) {
        this.logger.warn({ botId: ctx.botId }, 'WebWidget gateway not initialized');
        return;
      }

      // Upload file to S3 with a temporary key
      const s3Key = `web-widget-files/${ctx.botId}/${Date.now()}-${fileName}`;
      await s3.send(
        new PutObjectCommand({
          Bucket: config.s3Bucket,
          Key: s3Key,
          Body: file,
          ContentType: mimeType,
        }),
      );

      // Generate presigned URL (1 hour expiry)
      const url = await getSignedUrl(
        s3,
        new GetObjectCommand({
          Bucket: config.s3Bucket,
          Key: s3Key,
        }),
        { expiresIn: 3600 },
      );

      const isImage = mimeType.startsWith('image/');
      const message = {
        type: isImage ? 'image' : 'file',
        url,
        fileName,
        mimeType,
        sender: 'bot',
        timestamp: new Date().toISOString(),
      };

      // Try specific connection first
      let sent = false;
      if (ctx.webWidgetChannelId && ctx.webWidgetUserId) {
        sent = this.gateway.pushToConnection(ctx.webWidgetChannelId, ctx.webWidgetUserId, message);
      }
      if (!sent) {
        sent = this.gateway.pushToGroup(ctx.groupJid, message);
      }

      if (sent) {
        this.logger.info(
          { botId: ctx.botId, groupJid: ctx.groupJid, fileName },
          'WebWidget file pushed to connection',
        );
      } else {
        this.logger.warn(
          { botId: ctx.botId, groupJid: ctx.groupJid },
          'No active web-widget connection found for file delivery',
        );
      }

      // Send caption as a separate message if provided
      if (caption) {
        await this.sendReply(ctx, caption);
      }
    } catch (err) {
      this.logger.error(
        { err, botId: ctx.botId, groupJid: ctx.groupJid, fileName },
        'Failed to send file via web-widget channel',
      );
    }
  }
}
