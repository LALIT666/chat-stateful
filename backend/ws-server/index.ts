//mene bun add ws @types/ws redis kara okay

import { WebSocket, WebSocketServer } from "ws";
import { createClient, type RedisClientType } from "redis";

// ========================
// ! TYPES
// ========================

interface JoinMessage {
  type: "join";
  roomId: string;
}

interface ChatMessage {
  type: "chat";
  roomId: string;
}

interface RedisPayload {
  roomId: string;
  message: string;
  senderId: string;
}

type IncomingMessage = JoinMessage | ChatMessage;
