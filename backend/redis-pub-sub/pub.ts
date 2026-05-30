import { createClient } from "redis";
const pub = createClient();
await pub.connect();
setInterval(() => {
  pub.publish("chat-room", "this is msg in the chat-room channel");
}, 5000);
