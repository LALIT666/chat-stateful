import { createClient } from "redis";
const sub = createClient();
await sub.connect();
await sub.subscribe("chat-room", (msg) => {
  console.log("MSG Recived: ", msg);
});
