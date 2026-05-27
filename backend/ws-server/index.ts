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
  message: string;
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

// ========================
// ! REDIS SUBSCRIBE HANDLER
// ========================

async function subscribeToRoom(roomId: string): Promise<void> {
  //jab bhi koi roomid par message bheje toh ye callback chala dena okay  .subscribe matlab hum uss room ko suno
  await subscriber.subscribe(roomId, (data: string) => {
    const parsed: RedisPayload = JSON.parse(data); //ab jo ye yaha par parsed hai usme roomid , senderid , message hai

    //ab humne kya kara hai ki humne roomid se un sara users ka set pata kar liya hai jo use room(roomid) me hai jaise ex-- roomid1 me Set { "user_A", "user_B" } ye hai toh roomUser me  Set { "user_A", "user_B" } ye cheez aa jayagi
    const roomUsers = rooms.get(parsed.roomId);

    //agar room me ek bhi user nahi hai toh return
    if (!roomUsers) return;

    for (const userId of roomUsers) {
      //Har userId ke liye uska full user object nikalo (socket ke saath)
      const user = users.get(userId);
      if (!user) continue;

      //Sender ko khud ka message mat bhejo
      if (user.userId === parsed.senderId) continue;

      // readyState ek number hai jo batata hai ki socket abhi kis state me hai.
      //CONNECTING = 0 --> Abhi connect ho raha hai (handshake chal raha hai) WebSocket.OPEN = 1
      //OPEN = 1	--> ✅ Connected hai, message bhej/le sakte hain
      //CLOSING	= 2	--> Abhi close ho raha hai
      //CLOSED = 3	❌ Connection band ho gaya
      if (user.socket.readyState === WebSocket.OPEN) {
        user.socket.send(
          JSON.stringify({
            type: "chat",
            roomId: parsed.roomId,
            message: parsed.message,
          }),
        );
      }
    }
  });
}

// ========================
// ! HANDLERS
// ========================

function generateId(): string {
  return Math.random().toString(36).substring(2, 10);
}

function handleJoin(user: UserConnection, roomId: string): void {
  user.rooms.add(roomId);

  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Set());

    //matlab ki first time room hum bana rahe hai and toh phir subscribe karo
    subscribeToRoom(roomId);
  }

  rooms.get(roomId)?.add(user.userId);
  console.log(`👤 ${user.userId} joined room: ${roomId}`);
}

async function handleChat(
  user: UserConnection,
  roomId: string,
  message: string,
): Promise<void> {
  if (!user.rooms.has(roomId)) {
    user.socket.send(
      JSON.stringify({ type: "error", message: "You are not in this room" }),
    );

    return;
  }

  const payload: RedisPayload = {
    roomId,
    message,
    senderId: user.userId,
  };

  await publisher.publish(roomId, JSON.stringify(payload));
}

function handleDisconnect(userId: string): void {
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
  console.log(`❌ ${userId} disconnected`);
}

// ========================
// ! WEBSOCKET SERVER
// ========================

const PORT = parseInt(process.env.PORT || "8080");

async function main(): Promise<void> {
  await setupRedis();

  const wss = new WebSocketServer({ port: PORT });

  wss.on("connection", (socket: WebSocket) => {
    const userId = generateId();

    const user: UserConnection = {
      socket,
      userId,
      rooms: new Set(),
    };

    users.set(userId, user);
    console.log(`user with userId: ${userId} connected successfully`);

    socket.send(JSON.stringify({ type: "connected", userId }));

    socket.on("message", async (raw: Buffer) => {
      try {
        //jo buffer aaya usko  string me conver kara and then usko json obj me
        const data: IncomingMessage = JSON.parse(raw.toString());

        switch (data.type) {
          case "join":
            handleJoin(user, data.roomId);

            break;
          case "chat":
            await handleChat(user, data.roomId, data.message);
            break;

          default:
            socket.send(
              JSON.stringify({ type: "error", message: "Unknown type" }),
            );
            break;
        }
      } catch (error) {
        socket.send(JSON.stringify({ type: "error", message: "Invalid JOSN" }));
      }
    });

    console.log(`🚀 WS server running on port ${PORT}`);
  });
}

main();
