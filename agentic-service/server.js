const http = require("http");
const crypto = require("crypto");

const PORT = Number(process.env.PORT || 8004);
const sessions = new Map();

function sendJson(res, status, body) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

async function readJson(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  if (!chunks.length) return {};
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

function getSession(sessionId) {
  if (!sessions.has(sessionId)) {
    sessions.set(sessionId, { id: sessionId, name: "New Chat", lastActive: new Date().toISOString(), messages: [] });
  }
  return sessions.get(sessionId);
}

http
  .createServer(async (req, res) => {
    try {
      const path = req.url.split("?")[0];
      if (req.method === "GET" && path === "/status") {
        return sendJson(res, 200, { service: "agentic-service", status: "OK" });
      }
      if (req.method === "GET" && path === "/chat/sessions") {
        return sendJson(res, 200, {
          sessions: Array.from(sessions.values())
            .map(({ id, name, lastActive }) => ({ id, name, lastActive }))
            .sort((a, b) => b.lastActive.localeCompare(a.lastActive))
        });
      }
      if (req.method === "GET" && path.startsWith("/chat/") && path.endsWith("/history")) {
        const sessionId = path.split("/")[2];
        return sendJson(res, 200, { sessionId, messages: getSession(sessionId).messages });
      }
      if (req.method === "POST" && path === "/chat/message") {
        const { sessionId = crypto.randomUUID(), message = "" } = await readJson(req);
        const session = getSession(sessionId);
        session.name = session.name === "New Chat" && message ? String(message).slice(0, 40) : session.name;
        session.lastActive = new Date().toISOString();
        session.messages.push({ role: "user", content: message, createdAt: session.lastActive });
        const reply = "RentPi assistant is running. Connect an LLM provider here for grounded rental answers.";
        session.messages.push({ role: "assistant", content: reply, createdAt: new Date().toISOString() });
        return sendJson(res, 200, { sessionId, reply, messages: session.messages });
      }
      sendJson(res, 404, { error: "Route not found" });
    } catch (error) {
      sendJson(res, 500, { error: "agentic-service error", detail: error.message });
    }
  })
  .listen(PORT, () => console.log(`agentic-service listening on ${PORT}`));
