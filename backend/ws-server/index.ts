import { WebSocket, WebSocketServer } from "ws";
import { type RedisClientType, createClient } from "redis";

const PORT = parseInt(process.env.PORT || "8080");

interface JoinMessage {
  type: "join_room";
  roomId: string;
}

interface ChatMessage {
  type: "chat";
  roomId: string;
  message: string;
}

type IncommingMessage = JoinMessage | ChatMessage;

interface RedisPayload {
  roomId: string;
  senderId: string;
  message: string;
}

interface UserConnection {
  socket: WebSocket;
  userId: string;
  rooms: Set<string>;
}

const users: Map<string, UserConnection> = new Map();

const rooms: Map<string, Set<string>> = new Map();

const publisher: RedisClientType = createClient();
const subscriber: RedisClientType = createClient();

async function setupRedis() {
  console.log("Redis connection...");
  await publisher.connect();
  console.log("Redis publisher connecte successfully");
  await subscriber.connect();
  console.log("Redis subscriber connected successfully");
  console.log("Redis connectd ✅");
}

async function subscribeRoom(roomId: string): Promise<void> {
  await subscriber.subscribe(roomId, (data: string) => {
    const parsedData: RedisPayload = JSON.parse(data);

    const roomUsers = rooms.get(parsedData.roomId);
    if (!roomUsers) return;

    for (const userIdInRoom of roomUsers) {
      const user = users.get(userIdInRoom);
      if (!user) continue;
      if (user.userId === parsedData.senderId) continue;

      if (user.socket.readyState === WebSocket.OPEN) {
        user.socket.send(
          JSON.stringify({
            type: "chat",
            message: parsedData.message,
            roomId: parsedData.roomId,
          }),
        );
      }
    }
  });
}

function generateId() {
  return Math.random().toString(36).substring(2, 10);
}

async function handleJoin(user: UserConnection, roomId: string) {
  user.rooms.add(roomId);

  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Set());
    await subscribeRoom(roomId);
  }

  rooms.get(roomId)?.add(user.userId);
  console.log(
    `user with userId: ${user.userId} join the room with roomId: ${roomId}`,
  );
}

async function handleChat(
  user: UserConnection,
  roomId: string,
  message: string,
): Promise<void> {
  if (!user.rooms.has(roomId)) {
    user.socket.send(
      JSON.stringify({
        type: "error",
        message: "you are not part of this room",
      }),
    );
    return;
  }
  const publisherPayload: RedisPayload = {
    roomId,
    senderId: user.userId,

    message,
  };
  await publisher.publish(roomId, JSON.stringify(publisherPayload));
}

async function disConnect(userId: string) {
  const user = users.get(userId);
  if (!user) return;
  for (const roomId of user.rooms) {
    const roomUsers = rooms.get(roomId);
    if (roomUsers) {
      roomUsers.delete(userId);
      if (roomUsers.size === 0) {
        await subscriber.unsubscribe(roomId);
        rooms.delete(roomId);
      }
    }
  }

  users.delete(userId);
  console.log(`❌ ${userId} disconnected`);
}

const wss = new WebSocketServer({ port: PORT });

async function main() {
  await setupRedis();
  console.log(`WebSocket Server running on port ${PORT}`);
  wss.on("connection", (socket: WebSocket) => {
    console.log("New client connected");
    const userId = generateId();

    const user: UserConnection = {
      socket,
      userId,
      rooms: new Set(),
    };

    users.set(userId, user);

    socket.send(
      JSON.stringify({
        type: "connected",
        message: `User connected successfully 👍 with userId: ${userId}`,
      }),
    );
    socket.on("message", (data: Buffer) => {
      try {
        const parsedData: IncommingMessage = JSON.parse(data.toString());

        switch (parsedData.type) {
          case "join_room":
            handleJoin(user, parsedData.roomId);
            break;
          case "chat":
            handleChat(user, parsedData.roomId, parsedData.message);
            break;

          default:
            socket.send(
              JSON.stringify({ type: "error", message: "UNKNOWN TYPE" }),
            );
            break;
        }
      } catch (error) {
        console.error("error ", error);
        socket.send(JSON.stringify({ type: "error", message: "invalid JSON" }));
      }
    });

    socket.on("close", () => disConnect(userId));
  });
}

main();
