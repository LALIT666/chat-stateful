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
