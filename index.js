import http from "http";
import fs from "fs";
import { WebSocket, WebSocketServer } from "ws";
import Redis from "ioredis";
const pub = new Redis();
const sub = new Redis();
const server = http.createServer((req, res) => {
  if (req.url === "/" && req.method === "GET") {
    fs.readFile("./index.html", (err, data) => {
      if (err) { res.writeHead(500); return res.end("Error loading page"); }
      res.writeHead(200, { "Content-Type": "text/html" });
      res.end(data);
    });
    return;
  }
  res.writeHead(404);
  res.end("Not found");
});


const channelSubscribers= new Map();
sub.on("message", (channel, message) => {
  for (const ws of channelSubscribers.get(channel) ?? []) {
    if (ws.readyState === WebSocket.OPEN) ws.send(message); // already a JSON string
  }
});

async function subscribemychannel(channelName, ws) {
  let subscribers = channelSubscribers.get(channelName);
  if (!subscribers) {
    subscribers = new Set();
    channelSubscribers.set(channelName, subscribers);
    await sub.subscribe(channelName);
  }
  subscribers.add(ws);
  ws.channels.add(channelName);
}

async function unsubscribemychannel(channelName, ws) {
  const subscribers = channelSubscribers.get(channelName);
  if (!subscribers) return;
  subscribers.delete(ws);
  ws.channels.delete(channelName);
  if (subscribers.size === 0) {
    channelSubscribers.delete(channelName);
    await sub.unsubscribe(channelName);
  }
}

async function publishmessage(channelName, from, text) {
  await pub.publish(channelName, JSON.stringify({
    type: "message",
    channel: channelName,
    from,
    text: String(text ?? "").slice(0, 1000),
  }));
 
}

const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
  ws.name = "Anonymous";
  ws.channels = new Set();

      ws.on("message", async(raw) => {
        let data;
        try { data = JSON.parse(raw.toString()); } catch { return; }

        switch (data.operation) {
      case "setName":
        ws.name = String(data.name || "Anonymous").slice(0, 30);
        return;
      case "createChannel":
      case "subscribeChannel":
        await subscribemychannel(data.channelName, ws);
        return;
      case "unsubscribeChannel":
        await unsubscribemychannel(data.channelName, ws);
        return;
      case "PublishMessage":
        if (!ws.channels.has(data.channelName)) return; // must have joined
        await publishmessage(data.channelName, ws.name, data.text);
        return;
    }

  });

  ws.on("close",()=>{
    ws.channels.forEach(channel=>{
      unsubscribemychannel(channel,ws);
    })
  })
});

server.listen(8080, () => console.log("http://localhost:8080/"));