// Web Widget Gateway Manager
// Manages WebSocket server connections for the web-widget embeddable chat channel.
// Follows the same lifecycle pattern as WebGatewayManager but with:
//   - groupJid prefix 'ww#' (instead of 'web#')
//   - ownerUserId on each connection (for S3 scoping)
//   - External message handlers (./message-handler.ts)
//
// Connections are stored in-memory on the leader instance.
// JWT verification uses HMAC-SHA256 with the channel's clientSecret.

import { S3Client } from '@aws-sdk/client-s3';
import { SQSClient } from '@aws-sdk/client-sqs';
import {
  SecretsManagerClient,
  GetSecretValueCommand,
} from '@aws-sdk/client-secrets-manager';
import type pino from 'pino';
import type { WebSocket } from 'ws';
import type { ChannelConfig } from '@clawbot/shared';
import { config } from '../config.js';
import { getChannelsByType } from '../services/dynamo.js';
import { getCachedBot } from '../services/cached-lookups.js';
import {
  handleSendMessage,
  handleRequestUploadUrl,
  handleGetHistory,
  type WebWidgetInboundMessage,
  type MessageHandlerDeps,
} from './message-handler.js';

// -- Clients ------------------------------------------------------------------

const s3 = new S3Client({ region: config.region });
const sqs = new SQSClient({ region: config.region });
const secretsMgr = new SecretsManagerClient({ region: config.region });

// -- Types --------------------------------------------------------------------

export interface WebWidgetConnection {
  ws: WebSocket;
  channelId: string;
  userId: string;
  userName: string;
  botId: string;
  ownerUserId: string;  // bot owner's userId (for S3 scoping)
  groupJid: string;     // ww#{channelId}#{userId}
}

interface JwtPayload {
  sub?: string;
  userId?: string;
  name?: string;
  userName?: string;
  exp?: number;
  iat?: number;
  [key: string]: unknown;
}

// -- JWT Verification (native Web Crypto, HMAC-SHA256) ------------------------

async function verifyJwt(token: string, secret: string): Promise<JwtPayload> {
  const parts = token.split('.');
  if (parts.length !== 3) throw new Error('Invalid JWT format');
  const [headerB64, payloadB64, signatureB64] = parts;

  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['verify'],
  );
  const data = encoder.encode(`${headerB64}.${payloadB64}`);
  const sig = Buffer.from(
    signatureB64.replace(/-/g, '+').replace(/_/g, '/'),
    'base64',
  );
  const valid = await crypto.subtle.verify('HMAC', key, sig, data);
  if (!valid) throw new Error('Invalid JWT signature');

  const payload: JwtPayload = JSON.parse(
    Buffer.from(
      payloadB64.replace(/-/g, '+').replace(/_/g, '/'),
      'base64',
    ).toString(),
  );
  if (payload.exp && payload.exp < Math.floor(Date.now() / 1000)) {
    throw new Error('Token expired');
  }
  return payload;
}

// -- WebWidgetGatewayManager --------------------------------------------------

export class WebWidgetGatewayManager {
  private logger: pino.Logger;
  private connections = new Map<string, WebWidgetConnection>();
  private channelCache = new Map<string, { channel: ChannelConfig; secret: string }>();
  private stopped = false;
  private _isLeader = false;
  private deps: MessageHandlerDeps;

  constructor(parentLogger: pino.Logger) {
    this.logger = parentLogger.child({ component: 'web-widget-gateway' });
    this.deps = {
      logger: this.logger,
      s3,
      sqs,
    };
  }

  /** Set leader status (called by WebWidgetAdapter on leadership changes). */
  setLeaderStatus(leader: boolean): void {
    this._isLeader = leader;
  }

  /** Check whether this instance is the current leader. */
  isLeader(): boolean {
    return this._isLeader;
  }

  // -- Lifecycle --------------------------------------------------------------

  async start(): Promise<void> {
    this.stopped = false;

    // Pre-load web-widget channels so handleConnection can verify tokens
    await this.reloadChannels();

    const count = this.channelCache.size;
    if (count === 0) {
      this.logger.info('No web-widget channels configured, gateway idle');
    } else {
      this.logger.info({ channelCount: count }, 'Web-widget gateway started, ready for connections');
    }
  }

  async stop(): Promise<void> {
    this.stopped = true;

    for (const [connId, conn] of this.connections) {
      try {
        conn.ws.close(1001, 'Server shutting down');
      } catch (err) {
        this.logger.error({ err, connId }, 'Error closing WebSocket connection');
      }
    }

    this.connections.clear();
    this.channelCache.clear();
    this.logger.info('Web-widget gateway stopped, all connections closed');
  }

  /**
   * Reload web-widget channel configs from DynamoDB + Secrets Manager.
   * Builds a new cache and swaps atomically (no window where cache is empty).
   * Called at startup and when channels change in bulk.
   */
  async reloadChannels(): Promise<void> {
    const channels = await getChannelsByType('web-widget');
    const newCache = new Map<string, { channel: ChannelConfig; secret: string }>();

    for (const ch of channels) {
      try {
        const creds = await this.loadCredentials(ch.credentialSecretArn);
        if (creds.clientId && creds.clientSecret) {
          newCache.set(creds.clientId, {
            channel: ch,
            secret: creds.clientSecret,
          });
        }
      } catch (err) {
        this.logger.warn({ err, botId: ch.botId }, 'Failed to load web-widget channel credentials');
      }
    }

    this.channelCache = newCache;
  }

  /**
   * Hot-load a single channel into the cache (e.g. after channel creation).
   */
  async addChannel(channelId: string): Promise<void> {
    const channels = await getChannelsByType('web-widget');
    const ch = channels.find((c) => c.channelId === channelId);
    if (!ch) {
      this.logger.warn({ channelId }, 'addChannel: channel not found in DynamoDB');
      return;
    }
    try {
      const creds = await this.loadCredentials(ch.credentialSecretArn);
      if (creds.clientId && creds.clientSecret) {
        this.channelCache.set(creds.clientId, {
          channel: ch,
          secret: creds.clientSecret,
        });
        this.logger.info({ channelId, botId: ch.botId }, 'Web-widget channel added to cache');
      }
    } catch (err) {
      this.logger.warn({ err, channelId }, 'Failed to load credentials for new web-widget channel');
    }
  }

  /**
   * Remove a single channel from the cache and close its connections.
   */
  async removeChannel(channelId: string): Promise<void> {
    // Find and remove from cache by channelId
    for (const [clientId, entry] of this.channelCache) {
      if (entry.channel.channelId === channelId) {
        this.channelCache.delete(clientId);
        this.logger.info({ channelId, clientId }, 'Web-widget channel removed from cache');
        break;
      }
    }

    // Close any active connections for this channel
    for (const [connId, conn] of this.connections) {
      if (conn.channelId === channelId) {
        try {
          conn.ws.close(1001, 'Channel removed');
        } catch { /* ignore */ }
        this.connections.delete(connId);
      }
    }
  }

  // -- Connection Handling ----------------------------------------------------

  /**
   * Handle a new WebSocket connection on /ws/widget.
   * Query params: ?clientId=xxx&token=jwt
   */
  async handleConnection(ws: WebSocket, request: { url?: string }): Promise<void> {
    if (this.stopped) {
      ws.close(1013, 'Server shutting down');
      return;
    }

    if (!this._isLeader) {
      ws.close(1013, 'Not the leader instance, please reconnect');
      return;
    }

    // Parse query params from URL
    const url = new URL(request.url || '/', 'http://localhost');
    const clientId = url.searchParams.get('clientId');
    const token = url.searchParams.get('token');

    if (!clientId || !token) {
      ws.close(4001, 'Missing clientId or token');
      return;
    }

    // Look up channel config by clientId
    const cached = this.channelCache.get(clientId);
    if (!cached) {
      ws.close(4002, 'Unknown clientId');
      return;
    }

    // Verify JWT token
    let claims: JwtPayload;
    try {
      claims = await verifyJwt(token, cached.secret);
    } catch (err) {
      this.logger.warn({ err, clientId }, 'JWT verification failed');
      ws.close(4003, 'Authentication failed');
      return;
    }

    const userId = claims.sub || claims.userId || 'anonymous';
    const userName = claims.name || claims.userName || claims.sub || 'User';
    const channelId = cached.channel.channelId;
    const botId = cached.channel.botId;
    const groupJid = `ww#${channelId}#${userId}`;
    const connId = `${channelId}:${userId}:${Date.now().toString(36)}`;

    // Look up bot config to get ownerUserId (for S3 scoping)
    let ownerUserId = '';
    try {
      const bot = await getCachedBot(botId);
      ownerUserId = bot?.userId ?? '';
    } catch (err) {
      this.logger.warn({ err, botId }, 'Failed to look up bot owner');
    }

    // Store connection
    const conn: WebWidgetConnection = {
      ws,
      channelId,
      userId,
      userName,
      botId,
      ownerUserId,
      groupJid,
    };
    this.connections.set(connId, conn);

    this.logger.info({ connId, botId, userId, channelId }, 'Web-widget client connected');

    // Send welcome message
    this.send(connId, {
      type: 'connected',
      connectionId: connId,
      userId,
      userName,
    });

    // Register event handlers
    ws.on('message', (data: Buffer | string) => {
      void this.handleMessage(connId, data);
    });

    ws.on('close', () => {
      this.handleClose(connId);
    });

    ws.on('error', (err) => {
      this.logger.error({ err, connId }, 'WebSocket error');
      this.handleClose(connId);
    });
  }

  // -- Message Handling -------------------------------------------------------

  private async handleMessage(connId: string, rawData: Buffer | string): Promise<void> {
    const conn = this.connections.get(connId);
    if (!conn) return;

    let parsed: Record<string, unknown>;
    try {
      const str = typeof rawData === 'string' ? rawData : rawData.toString('utf-8');
      parsed = JSON.parse(str);
    } catch {
      this.send(connId, { type: 'error', message: 'Invalid JSON' });
      return;
    }

    const action = parsed.action as string | undefined;
    const msg = parsed as unknown as WebWidgetInboundMessage;

    switch (action) {
      case 'sendMessage':
        return handleSendMessage(connId, conn, msg, this.deps);
      case 'requestUploadUrl':
        return handleRequestUploadUrl(connId, conn, msg, this.deps);
      case 'getHistory':
        return handleGetHistory(conn, msg.limit, this.deps);
      default:
        this.send(connId, { type: 'error', message: `Unknown action: ${action}` });
    }
  }

  // -- Push to Connection -----------------------------------------------------

  /**
   * Push a message to a specific web-widget client identified by channelId + userId.
   * Called by WebWidgetAdapter.sendReply() to deliver bot responses.
   */
  pushToConnection(
    channelId: string,
    userId: string,
    message: Record<string, unknown>,
  ): boolean {
    let sent = false;
    for (const [, conn] of this.connections) {
      if (conn.channelId === channelId && conn.userId === userId) {
        this.safeSend(conn.ws, message);
        sent = true;
      }
    }
    return sent;
  }

  /**
   * Push a message to ALL connections for a given groupJid.
   * Used when the adapter only has botId + groupJid (reply consumer path).
   */
  pushToGroup(groupJid: string, message: Record<string, unknown>): boolean {
    let sent = false;
    for (const [, conn] of this.connections) {
      if (conn.groupJid === groupJid) {
        this.safeSend(conn.ws, message);
        sent = true;
      }
    }
    return sent;
  }

  // -- Disconnect -------------------------------------------------------------

  private handleClose(connId: string): void {
    const conn = this.connections.get(connId);
    if (!conn) return;

    this.connections.delete(connId);
    this.logger.info({ connId, botId: conn.botId, userId: conn.userId }, 'Web-widget client disconnected');
  }

  // -- Helpers ----------------------------------------------------------------

  /**
   * Send a message to a connection by connId.
   * This method is also exposed via deps.send for use by external message handlers.
   */
  private send(connId: string, message: Record<string, unknown>): void {
    const conn = this.connections.get(connId);
    if (!conn) return;
    this.safeSend(conn.ws, message);
  }

  private safeSend(ws: WebSocket, data: unknown): void {
    try {
      if (ws.readyState === 1 /* OPEN */) {
        ws.send(JSON.stringify(data));
      }
    } catch (err) {
      this.logger.warn({ err }, 'Failed to send WebSocket message');
    }
  }

  private async loadCredentials(
    secretArn: string,
  ): Promise<Record<string, string>> {
    const res = await secretsMgr.send(
      new GetSecretValueCommand({ SecretId: secretArn }),
    );
    if (!res.SecretString) {
      throw new Error(`Secret ${secretArn} has no SecretString`);
    }
    return JSON.parse(res.SecretString);
  }

  get isActive(): boolean {
    return this.connections.size > 0;
  }
}

// -- Singleton ----------------------------------------------------------------

let _manager: WebWidgetGatewayManager | null = null;

export function getWebWidgetGatewayManager(): WebWidgetGatewayManager | null {
  return _manager;
}

export function initWebWidgetGatewayManager(logger: pino.Logger): WebWidgetGatewayManager {
  _manager = new WebWidgetGatewayManager(logger);
  return _manager;
}
