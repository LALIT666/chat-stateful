// 2nd
import { WebSocket, WebSocketServer } from "ws";
import { createClient, type RedisClientType } from "redis";

const publisher: RedisClientType = createClient();
const subscriber: RedisClientType = createClient();

async function setupRedis(): Promise<void> {
  await publisher.connect();
  await subscriber.connect();
  console.log("Redis connected ✅");
}

interface RedisPayLoad {
  message: string;
  roomId: string;
  j;
  senderId: string;
}

interface JoinMessage {
  type: "join_room";
  roomId: string;
}

interface ChatMessage {
  type: "chat";
  roomId: string;
  message: string;
}

type IncomingMessage = ChatMessage | JoinMessage;

interface UserConnection {
  socket: WebSocket;
  userId: string;
  rooms: Set<string>;
}

//userid userconnection object
const users: Map<string, UserConnection> = new Map();

//roomid , set of usersids
const rooms: Map<string, Set<string>> = new Map();

const PORT = parseInt(process.env.PORT || "8080");

const wss = new WebSocketServer({ port: PORT });

async function subscribeToRoom(roomId: string): Promise<void> {
  await subscriber.subscribe(roomId, (data: string) => {
    const parsedData: RedisPayLoad = JSON.parse(data);

    const roomUsers = rooms.get(roomId);

    if (!roomUsers) return;

    for (const userId of roomUsers) {
      const user = users.get(userId);
      if (!user) continue;
      if (parsedData.senderId === user.userId) continue;

      if (user.socket.readyState === WebSocket.OPEN) {
        user.socket.send(
          JSON.stringify({
            type: "chat",
            roomId: parsedData.roomId,
            message: parsedData.message,
          }),
        );
      }
    }
  });
}

function generateId() {
  return Math.random().toString(36).substring(2, 10);
}

function handleJoinRoom(user: UserConnection, roomId: string) {
  user.rooms.add(roomId);

  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Set());
    subscribeToRoom(roomId);
  }

  rooms.get(roomId)?.add(user.userId);
  console.log(`${user.userId}, roomid: ${roomId}`);
}

async function handleChat(
  user: UserConnection,
  message: string,
  roomId: string,
): Promise<void> {
  if (!user.rooms.has(roomId)) {
    user.socket.send(
      JSON.stringify({
        type: "error",
        message: "you are not the part of this room",
      }),
    );
    return;
  }

  const publishPayload: RedisPayLoad = {
    roomId,
    message,
    senderId: user.userId,
  };

  await publisher.publish(roomId, JSON.stringify(publishPayload));
}

//
function handleDisconnect(userId: string) {
  const user = users.get(userId);
  if (!user) return;
  for (const roomId of user.rooms) {
    const roomUsers = rooms.get(roomId);
    if (roomUsers) {
      roomUsers.delete(userId);
      if (roomUsers.size === 0) {
        rooms.delete(roomId);
        subscriber.unsubscribe(roomId);
      }
    }
  }

  users.delete(userId);
  console.log(`${userId}, disconnected`);
}

async function main(): Promise<void> {
  wss.on("connection", (socket: WebSocket) => {
    const userId = generateId();

    const user: UserConnection = {
      socket,
      userId,
      rooms: new Set(),
    };

    users.set(userId, user);

    console.log(`user with userId: ${userId} is connected`);

    socket.send(JSON.stringify({ type: "connected", userId }));

    socket.on("message", (rawData: Buffer) => {
      const dataParsed: IncomingMessage = JSON.parse(rawData.toString());

      switch (dataParsed.type) {
        case "chat":
          handleChat(user, dataParsed.message, dataParsed.roomId);
          break;
        case "join_room":
          handleJoinRoom(user, dataParsed.roomId);
          break;

        default:
          socket.send(
            JSON.stringify({ type: "error", message: "UNKNOWN TYPE" }),
          );
          break;
      }
    });
    console.log("New client connected");

    socket.on("close", () => handleDisconnect(userId));
  });

  console.log(`WS Server is running on port: ${PORT}`);
}

main();
