# Web Chat Widget + Web Channel Enhancement — Design Document

**Date:** 2026-03-31
**Goal:** 1) 创建独立 CDK + Amplify 项目部署可嵌入的 WebSocket 聊天 Widget；2) 在 nanoclaw 新增 `web-widget` channel（参照 DingTalk/Feishu 模式），支持双向多媒体消息（文字/图片/文件）。

## Background

用户希望用自有的轻量 Web 聊天界面替代钉钉/飞书等第三方 IM，作为 channel 注册到 nanoclaw 后与 Bot 对话。架构文档 `08-channel-management.md` 中列出的 5 个 channel（Telegram、Discord、Slack、Feishu、DingTalk）均为第三方 IM 平台。需要新增一个 `web-widget` channel 类型，遵循与 DingTalk/Feishu 完全一致的适配器模式。

### 设计原则

- **纯新增**：不修改现有任何代码文件，仅新增文件和扩展类型
- **遵循现有模式**：参照 `dingtalk/` 和 `feishu/` 的两级目录结构
- **独立项目**：聊天 Widget 作为独立 CDK 项目，不依赖 nanoclaw 代码库

## Scope

### 项目 1：`clawbot-web-widget`（新独立 CDK 项目）

- 路径：`/root/keith-space/github-search/clawbot-web-widget/`
- React 19 聊天 Widget，Amplify Hosting 部署
- WebSocket 客户端直连 nanoclaw `/ws/widget` 端点
- 支持发送：文字、图片、文件
- 支持接收：文字、图片（内联预览）、文件（下载链接）
- 可通过 iframe 嵌入第三方系统
- Demo 级别，clientSecret 通过环境变量配置

### 项目 2：nanoclaw 新增 `web-widget` channel（纯新增，零修改）

- 新增 `web-widget` channel 适配器（参照 DingTalk/Feishu 目录模式）
- 新增 WebSocket 端点 `/ws/widget`（独立于现有 `/ws/chat`）
- 支持双向多媒体：文字 + 图片 + 文件（base64 内嵌 + presigned upload URL 双通道）
- 区分 image/file 回复类型
- Agent Runtime 无需改动（已有 `downloadAttachments()` + Read tool 图片支持）

### 不在范围内

- 用户注册/登录（嵌入方传入 userId）
- JS SDK 嵌入模式（后续扩展）
- 端到端加密
- 消息已读回执、typing indicator

## Architecture

### 整体架构

```
┌─────────────────────────────┐
│  CDK Stack (独立项目)        │
│  clawbot-web-widget         │
│                             │
│  ┌───────────────────────┐  │
│  │  Amplify Hosting      │  │
│  │  React Chat Widget    │  │
│  └───────────┬───────────┘  │
│              │              │
│  环境变量:                   │
│  VITE_NANOCLAW_WS_URL       │
│  VITE_CHANNEL_ID            │
│  VITE_CLIENT_SECRET         │
└──────────────┼──────────────┘
               │ WebSocket /ws/widget (新端点)
               │ JWT (HMAC-SHA256)
               ▼
┌──────────────────────────────────────────────────────┐
│  Nanoclaw Control Plane (ECS Fargate)                │
│                                                      │
│  🆕 WebWidgetGatewayManager (web-widget/)            │
│  ├─ /ws/widget WebSocket 端点                        │
│  ├─ JWT 认证 (clientId + clientSecret)               │
│  ├─ 解析 attachments (base64 → S3 / presigned URL)   │
│  └─ 构建 SQS payload (含 attachments[])              │
│                                                      │
│  🆕 WebWidgetAdapter (adapters/web-widget/)          │
│  ├─ Leader 选举 (DynamoDB lock)                      │
│  ├─ sendReply() → type: "message"                    │
│  └─ sendFile()  → type: "image" / "file"             │
│                                                      │
│  ──► SQS FIFO ──► AgentCore microVM                 │
│                    ├─ 图片: Read tool 查看            │
│                    └─ 文件: /workspace/group/附件/     │
│                                                      │
│  ◄── SQS Reply ◄── MCP send_message / send_file     │
└──────────────────────────────────────────────────────┘
```

### Nanoclaw 代码结构变更（纯新增）

```
control-plane/src/
├── adapters/
│   ├── base.ts                        # 不动
│   ├── registry.ts                    # 不动
│   ├── dingtalk/index.ts              # 不动
│   ├── discord/index.ts               # 不动
│   ├── feishu/index.ts                # 不动
│   ├── slack/index.ts                 # 不动
│   ├── telegram/index.ts              # 不动
│   ├── web/index.ts                   # 不动
│   └── web-widget/                    # 🆕 新建
│       └── index.ts                   # WebWidgetAdapter extends BaseChannelAdapter
├── dingtalk/                          # 不动
├── feishu/                            # 不动
├── web/                               # 不动
└── web-widget/                        # 🆕 新建
    ├── gateway-manager.ts             # WebSocket 连接管理 + 附件处理
    └── message-handler.ts             # 消息解析（文字/图片/文件）

shared/src/
└── types.ts                           # ChannelType 加 'web-widget'（扩展 union）
```

仅需改动的现有文件（纯 additive）：
- `shared/src/types.ts` — `ChannelType` union 加 `| 'web-widget'`
- `control-plane/src/index.ts` — `registry.register(new WebWidgetAdapter(logger))` 一行
- `control-plane/src/index.ts` — 注册 `/ws/widget` WebSocket 路由

### 使用流程

1. 用户在 nanoclaw 管理台创建 Bot
2. 为 Bot 添加 `web-widget` channel → 获得 `clientId` + `clientSecret`（自动生成）
3. 将 `clientId`、`clientSecret`、nanoclaw WebSocket URL 配到 CDK 项目环境变量
4. `cdk deploy` → 得到 Amplify 托管 URL
5. 嵌入方用 `<iframe src="https://xxx.amplifyapp.com?userId=user123">` 嵌入

## WebSocket Message Protocol

### 客户端 → 服务端（用户发送）

```jsonc
// 文字消息（已有，不变）
{ "action": "sendMessage", "text": "你好" }

// 带附件消息（新增）— 小文件 ≤256KB base64 内嵌
{
  "action": "sendMessage",
  "text": "请看这张图",
  "attachments": [
    {
      "fileName": "photo.jpg",
      "mimeType": "image/jpeg",
      "size": 245760,
      "data": "base64-encoded..."
    }
  ]
}

// 大文件上传请求（新增）— >256KB 走 HTTP presigned URL
{ "action": "requestUploadUrl", "fileName": "big-video.mp4", "mimeType": "video/mp4", "size": 10485760 }

// 大文件上传完成后发送消息（新增）— 引用 s3Key
{
  "action": "sendMessage",
  "text": "",
  "attachments": [
    { "s3Key": "web-uploads/channelId/timestamp-big-video.mp4", "fileName": "big-video.mp4", "mimeType": "video/mp4", "size": 10485760 }
  ]
}

// 获取历史消息（已有，不变）
{ "action": "getHistory", "limit": 50 }
```

### 服务端 → 客户端（Bot 回复）

```jsonc
// 连接确认（已有，不变）
{ "type": "connected", "connectionId": "xxx", "userId": "xxx", "userName": "xxx" }

// 消息确认（已有，不变）
{ "type": "ack", "messageId": "web-xxx" }

// 文字回复（已有，不变）
{ "type": "message", "text": "这是回复", "sender": "bot", "timestamp": "2026-03-31T..." }

// 图片回复（新增 type）
{ "type": "image", "url": "presigned-s3-url", "fileName": "chart.png", "mimeType": "image/png", "sender": "bot", "timestamp": "..." }

// 文件回复（已有，不变）
{ "type": "file", "url": "presigned-s3-url", "fileName": "report.pdf", "mimeType": "application/pdf", "sender": "bot", "timestamp": "..." }

// 上传 URL 响应（新增）
{ "type": "uploadUrl", "uploadUrl": "presigned-s3-put-url", "s3Key": "web-uploads/...", "expiresIn": 300 }

// 历史消息（已有，不变）
{ "type": "history", "messages": [...] }

// 错误（已有，不变）
{ "type": "error", "message": "error description" }
```

### 文件上传策略（双通道）

| 文件大小 | 通道 | 流程 |
|---------|------|------|
| ≤ 256KB | Base64 内嵌 WebSocket | `sendMessage` + `attachments[].data` → Gateway 解码 → S3 |
| > 256KB | Presigned Upload URL | `requestUploadUrl` → HTTP PUT S3 → `sendMessage` + `attachments[].s3Key` |

S3 路径格式：`{userId}/{botId}/attachments/{messageId}/{fileName}`（与已有 file-attachments 设计一致）

## 项目 1 详细设计：clawbot-web-widget

### CDK Stack

```
ClawbotWebWidgetStack
├── Amplify App (React, auto-build from repo)
│   └── Branch: main
│       └── 环境变量: VITE_NANOCLAW_WS_URL, VITE_CHANNEL_ID, VITE_CLIENT_SECRET
└── Outputs: AmplifyAppUrl
```

CDK 代码量约 100 行，仅包含 Amplify Hosting 资源。

### React 前端结构

```
src/
├── App.tsx                 # 路由入口，解析 URL params (userId)
├── components/
│   ├── ChatWidget.tsx      # 主聊天容器（可最小化浮窗）
│   ├── MessageList.tsx     # 消息列表（虚拟滚动）
│   ├── MessageBubble.tsx   # 单条消息气泡（文字/图片/文件）
│   ├── InputBar.tsx        # 输入栏 + 发送按钮 + 附件按钮
│   ├── FilePreview.tsx     # 文件/图片预览组件
│   └── ConnectionStatus.tsx # 连接状态指示器
├── hooks/
│   ├── useWebSocket.ts     # WebSocket 连接管理 + 自动重连
│   └── useFileUpload.ts    # 文件上传逻辑（base64/presigned 双通道）
├── lib/
│   ├── jwt.ts              # JWT token 生成（HMAC-SHA256）
│   └── config.ts           # 环境变量读取
└── types.ts                # 消息类型定义
```

### Widget UI

```
┌─────────────────────────────────────┐
│  ClawBot Chat                  ─  × │  ← 标题栏，可最小化
├─────────────────────────────────────┤
│                                     │
│  🤖 你好！有什么可以帮你？           │
│                            14:30    │
│                                     │
│           帮我分析这张图 📎          │
│           [photo.jpg 缩略图预览]     │
│                            14:31    │
│                                     │
│  🤖 这是一张架构图，我来分析...      │
│  🤖 [report.pdf 📄 点击下载]        │
│                            14:32    │
│                                     │
├─────────────────────────────────────┤
│ [📎] [输入消息...]          [发送▶] │
└─────────────────────────────────────┘
```

功能：
- 文字输入 + Enter 发送
- 附件按钮选择文件/图片
- 拖拽文件到聊天区域上传
- Ctrl+V 粘贴剪贴板图片
- 图片消息：内联缩略图 + 点击放大 lightbox
- 文件消息：文件名 + 图标 + 下载链接
- 消息历史加载（连接时自动 `getHistory`）
- 连接状态指示（连接中 / 已连接 / 断线重连中）
- 自动重连（指数退避，最大 30s）

嵌入方式：
```html
<iframe src="https://xxx.amplifyapp.com?userId=user123"
        style="width:400px;height:600px;border:none;" />
```

### 技术选型

| 技术 | 选择 | 理由 |
|------|------|------|
| 框架 | React 19 | 与 nanoclaw web-console 一致 |
| 构建 | Vite | 快速，Amplify 原生支持 |
| 样式 | TailwindCSS | 轻量，与 nanoclaw 一致 |
| WebSocket | 原生 WebSocket API | 无需额外库，Demo 够用 |
| JWT | Web Crypto API (HMAC-SHA256) | 浏览器原生，零依赖 |
| CDK | AWS CDK v2 | 与 nanoclaw infra 一致 |

## 项目 2 详细设计：Nanoclaw 新增 `web-widget` Channel

### 设计参照

参照 DingTalk 的实现模式（两级目录 + Leader 选举 + Gateway Manager）：

```
# DingTalk 模式（参照）
adapters/dingtalk/index.ts          → DingTalkAdapter (leader选举 + sendReply/sendFile)
dingtalk/gateway-manager.ts         → DWClient WebSocket 连接管理
dingtalk/message-handler.ts         → 消息解析 + 附件下载 + SQS 入队

# web-widget 新建（同模式）
adapters/web-widget/index.ts        → WebWidgetAdapter (leader选举 + sendReply/sendFile)
web-widget/gateway-manager.ts       → /ws/widget WebSocket 连接管理 + JWT 认证
web-widget/message-handler.ts       → 消息解析 + 附件处理(base64→S3) + SQS 入队
```

### 新增文件清单

| 文件 | 内容 | 参照 |
|------|------|------|
| `control-plane/src/adapters/web-widget/index.ts` | WebWidgetAdapter：Leader 选举、sendReply、sendFile | `adapters/dingtalk/index.ts` |
| `control-plane/src/web-widget/gateway-manager.ts` | WebSocket `/ws/widget` 端点、JWT 认证、连接池管理 | `dingtalk/gateway-manager.ts` |
| `control-plane/src/web-widget/message-handler.ts` | 消息解析、附件上传 S3、presigned URL 生成、SQS 入队 | `dingtalk/message-handler.ts` |

### 现有文件改动（纯 additive，不改现有逻辑）

| 文件 | 改动 |
|------|------|
| `shared/src/types.ts` | `ChannelType` union 加 `\| 'web-widget'` |
| `shared/src/channel-adapter.ts` | `ReplyContext` 加 `webWidgetChannelId?: string`、`webWidgetUserId?: string` |
| `control-plane/src/index.ts` | `registry.register(new WebWidgetAdapter(logger))` + 注册 `/ws/widget` 路由 |

### WebWidgetAdapter（adapters/web-widget/index.ts）

```typescript
export class WebWidgetAdapter extends BaseChannelAdapter {
  readonly channelType = 'web-widget' as const;
  private gateway: WebWidgetGatewayManager | null = null;

  // === Leader 选举（与 DingTalk/Feishu 同模式）===
  // DynamoDB lock: PK=__system__, SK=web-widget-gateway-leader
  // TTL: 30s, 续约: 15s, Standby 轮询: 15s

  async start(): Promise<void> {
    // tryAcquireLock() → becomeLeader() 或 startStandbyPoll()
  }

  private async becomeLeader(): Promise<void> {
    this.gateway = new WebWidgetGatewayManager(this.logger, /* deps */);
    await this.gateway.start();
  }

  // === 出站：sendReply（所有实例均可调用，无需 Leader）===
  async sendReply(ctx: ReplyContext, text: string): Promise<void> {
    const message = { type: 'message', text, sender: 'bot', timestamp: new Date().toISOString() };
    // 优先 pushToConnection(channelId, userId)，回退 pushToGroup(groupJid)
    this.gateway?.pushToConnection(ctx.webWidgetChannelId!, ctx.webWidgetUserId!, message)
      || this.gateway?.pushToGroup(ctx.groupJid, message);
  }

  // === 出站：sendFile（区分 image/file 类型）===
  async sendFile(ctx: ReplyContext, file: Buffer, fileName: string, mimeType: string, caption?: string): Promise<void> {
    const s3Key = `web-widget-files/${ctx.botId}/${Date.now()}-${fileName}`;
    await this.s3Upload(s3Key, file, mimeType);
    const url = await this.getPresignedUrl(s3Key);

    const isImage = mimeType.startsWith('image/');
    const message = {
      type: isImage ? 'image' : 'file',
      url, fileName, mimeType, sender: 'bot',
      timestamp: new Date().toISOString(),
    };
    this.gateway?.pushToConnection(ctx.webWidgetChannelId!, ctx.webWidgetUserId!, message);
    if (caption) await this.sendReply(ctx, caption);
  }
}
```

### WebWidgetGatewayManager（web-widget/gateway-manager.ts）

```typescript
export class WebWidgetGatewayManager {
  private connections = new Map<string, WebWidgetConnection>();

  // === WebSocket 连接处理 ===
  async handleConnection(ws: WebSocket, request: { url?: string }): Promise<void> {
    // 1. 解析 query: ?clientId=xxx&token=jwt
    // 2. 查找 channel config (channelType='web-widget', clientId 匹配)
    // 3. JWT 验证 (HMAC-SHA256, clientSecret from Secrets Manager)
    // 4. 提取 userId, 生成 groupJid = ww#{channelId}#{userId}
    // 5. 存储连接, 发送 { type: 'connected', connectionId, userId }
    // 6. 注册 message/close/error handlers
  }

  // === 入站消息路由 ===
  private async handleMessage(connId: string, rawData: Buffer | string): Promise<void> {
    const parsed = JSON.parse(rawData.toString());
    switch (parsed.action) {
      case 'sendMessage':
        return handleSendMessage(connId, this.connections.get(connId)!, parsed);
      case 'requestUploadUrl':
        return handleRequestUploadUrl(connId, this.connections.get(connId)!, parsed);
      case 'getHistory':
        return handleGetHistory(this.connections.get(connId)!, parsed.limit);
    }
  }

  // === 出站推送 ===
  pushToConnection(channelId: string, userId: string, message: Record<string, unknown>): boolean { /* ... */ }
  pushToGroup(groupJid: string, message: Record<string, unknown>): boolean { /* ... */ }
}
```

### Message Handler（web-widget/message-handler.ts）

```typescript
// === sendMessage 处理（参照 DingTalk message-handler 模式）===
export async function handleSendMessage(connId: string, conn: WebWidgetConnection, parsed: any) {
  const messageId = `ww-${Date.now()}-${randomId()}`;
  let attachments: Attachment[] = [];

  // 1. 附件处理
  if (parsed.attachments?.length) {
    for (const att of parsed.attachments) {
      const type = inferAttachmentType(att.mimeType); // image | audio | document | video

      if (att.data) {
        // base64 内嵌 (≤256KB) → 解码上传 S3
        const buffer = Buffer.from(att.data, 'base64');
        const s3Key = `${conn.userId}/${conn.botId}/attachments/${messageId}/${att.fileName}`;
        await s3Upload(s3Key, buffer, att.mimeType);
        attachments.push({ type, s3Key, mimeType: att.mimeType, fileName: att.fileName, size: att.size });
      } else if (att.s3Key) {
        // presigned upload 已完成 (>256KB)，直接引用
        attachments.push({ type, s3Key: att.s3Key, mimeType: att.mimeType, fileName: att.fileName, size: att.size });
      }
    }
  }

  // 2. 群组管理 — 配额检查 + 自动创建 Group
  const groupJid = `ww#${conn.channelId}#${conn.userId}`;
  await getOrCreateGroup(conn.botId, groupJid, conn.userName, 'web-widget', false);

  // 3. 存储 Message (DynamoDB, TTL 90 天)
  await storeMessage({ botId: conn.botId, groupJid, messageId, text: parsed.text, sender: 'user', attachments });

  // 4. 发送 ACK
  send(connId, { type: 'ack', messageId });

  // 5. 入队 SQS FIFO
  const sqsPayload: SqsInboundPayload = {
    type: 'inbound_message',
    botId: conn.botId,
    groupJid,
    userId: conn.ownerUserId,
    messageId,
    content: parsed.text || '',
    channelType: 'web-widget',
    timestamp: new Date().toISOString(),
    attachments: attachments.length ? attachments : undefined,
    replyContext: { webWidgetChannelId: conn.channelId, webWidgetUserId: conn.userId },
  };
  await sqsEnqueue(sqsPayload, `${conn.botId}#${groupJid}`);
}

// === requestUploadUrl 处理 ===
export async function handleRequestUploadUrl(connId: string, conn: WebWidgetConnection, parsed: any) {
  const s3Key = `${conn.userId}/${conn.botId}/attachments/${Date.now()}-${parsed.fileName}`;
  const uploadUrl = await getPresignedPutUrl(s3Key, parsed.mimeType, 300);
  send(connId, { type: 'uploadUrl', uploadUrl, s3Key, expiresIn: 300 });
}

// === 工具函数 ===
function inferAttachmentType(mimeType: string): 'image' | 'audio' | 'document' | 'video' {
  if (mimeType.startsWith('image/')) return 'image';
  if (mimeType.startsWith('video/')) return 'video';
  if (mimeType.startsWith('audio/')) return 'audio';
  return 'document';
}
```

### Agent Runtime 附件处理

**无需修改 agent-runtime 代码**。已有逻辑自动兼容 `web-widget` channel：
1. `handleInvocation()` 从 SQS payload 读取 `attachments[]`（已实现）
2. `downloadAttachments()` 下载附件到 `/workspace/group/attachments/{fileName}`（已实现，含路径安全校验）
3. Agent 通过 Claude Code 的 **Read tool** 查看图片文件（原生 multimodal 支持）
4. Agent 通过 Read tool 或 Bash tool 访问其他文件类型
5. SQS payload 中的 `attachments[]` 使用现有 `Attachment` 接口格式（`type`, `s3Key`, `mimeType`, `fileName?`, `size?`）

> 注意：当前 `query()` 只接受 string prompt，不支持 Anthropic API 原生 multimodal content blocks。
> 但 Agent 运行在 Claude Code 环境中，Read tool 原生支持读取图片并视觉理解，效果等同。

### 与 08-channel-management.md 对比

| | web-widget (新增) | DingTalk (参照) |
|---|---|---|
| 认证方式 | clientId + clientSecret (自动生成) | ClientID (AppKey) + ClientSecret (AppSecret) |
| 连接模式 | WebSocket `/ws/widget` + Leader 选举 | Stream 长连接 (DWClient) + Leader 选举 |
| 消息格式 | JSON `{ action, text, attachments }` | TOPIC_ROBOT Callback JSON |
| 签名验证 | JWT HMAC-SHA256 (clientSecret) | SDK 内部处理 |
| 群组支持 | 是 (每 userId 独立 group) | 是 (群聊 + 单聊) |
| 回复方式 | WebSocket pushToConnection / pushToGroup | REST API groupMessages/send |
| 特殊能力 | 图片内联预览、文件 presigned URL 下载 | Markdown、媒体上传下载 |
| 附件处理 | base64 内嵌 + presigned upload URL | downloadCode → REST download → S3 |

## Testing

### Widget 测试
- WebSocket 连接/断线重连
- 文字消息发送/接收
- 小文件 base64 上传 (≤256KB)
- 大文件 presigned URL 上传 (>256KB)
- 图片预览 + 文件下载
- iframe 嵌入模式 userId 传递

### Nanoclaw 测试
- WebWidgetAdapter Leader 选举 + failover
- JWT 认证（有效/过期/无效 token）
- Gateway 附件解析（base64 + s3Key 两种模式）
- Presigned upload URL 生成 + S3 上传验证
- SQS payload 附件字段传递 → Agent Runtime 下载
- sendReply / sendFile 区分 image/file 类型
- 多连接 pushToConnection / pushToGroup 路由

## Cost Impact

新 CDK 项目基本零成本：
- Amplify Hosting: 免费层 (5GB/月带宽, 1000 分钟构建)
- 无额外 Lambda / API Gateway / DynamoDB

Nanoclaw 侧无新增 AWS 资源，仅新增代码文件。DynamoDB Leader 选举复用现有 sessions 表。S3 存储费用随附件使用量线性增长（可忽略）。
