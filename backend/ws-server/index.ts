// 2nd
import { WebSocket, WebSocketServer } from "ws";

interface UserConnection {
  socket: WebSocket;
  userId: string;
  rooms: Set<string>;
}

const users: Map<string, UserConnection> = new Map();

const PORT = parseInt(process.env.PORT || "8080");

const wss = new WebSocketServer({ port: PORT });

function generateId() {
  return Math.random().toString(36).substring(2, 10);
}

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
    console.log("Got message: ", rawData.toString);
  });
  console.log("New client connected");
});

console.log(`WS Server is running on port: ${PORT}`);
