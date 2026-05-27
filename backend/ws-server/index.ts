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

// ========================
// ! STATE
// ========================

interface UserConnection {
  socket: WebSocket;
  userId: string;
  rooms: Set<string>;
}

const users: Map<string, UserConnection> = new Map();

//roomId --> set of userIds
const rooms: Map<string, Set<string>> = new Map();

// ========================
// ! REDIS SETUP
// ========================

//this is how you do redis setup okay

const publisher: RedisClientType = createClient();
const subscriber: RedisClientType = createClient();

async function setupRedis(): Promise<void> {
  await publisher.connect();
  console.log("Publisher connect successfully");
  await subscriber.connect();
  console.log("Subscriber connet successfully");

  console.log("Redis connected successfully ✅✅");
}
