const http = require("http");
const crypto = require("crypto");
const { MongoClient } = require("mongodb");

const PORT = Number(process.env.PORT || 8004);
const MONGO_URI = process.env.MONGODB_URI || process.env.MONGO_URI || "mongodb://mongodb:27017/rentpi_agentic";
const CENTRAL_API_URL = (process.env.CENTRAL_API_URL || "https://technocracy.brittoo.xyz").replace(/\/$/, "");
const CENTRAL_API_TOKEN = process.env.CENTRAL_API_TOKEN || "";
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";
const RENTAL_SERVICE_URL = (process.env.RENTAL_SERVICE_URL || "http://rental-service:8002").replace(/\/$/, "");
const ANALYTICS_SERVICE_URL = (process.env.ANALYTICS_SERVICE_URL || "http://analytics-service:8003").replace(/\/$/, "");

const rentPiKeywords = [
  "rental", "rent", "rented", "product", "products", "category", "categories",
  "pricing", "price", "availability", "available", "user", "users", "discount",
  "security score", "trend", "trends", "recommendation", "recommend", "peak",
  "surge", "busy", "boom", "booking", "inventory", "fleet"
];
let mongoClient;
let mongoDb;
let mongoConnectPromise;
let indexesReady = false;

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

async function connectMongo() {
  if (!mongoConnectPromise) {
    mongoConnectPromise = (async () => {
      mongoClient = new MongoClient(MONGO_URI);
      await mongoClient.connect();
      mongoDb = mongoClient.db();
      await ensureIndexes();
      indexesReady = true;
      return mongoDb;
    })();
  }

  await mongoConnectPromise;
  if (!indexesReady) {
    await ensureIndexes();
    indexesReady = true;
  }
  return mongoDb;
}

async function ensureIndexes() {
  const db = mongoDb;
  await db.collection("sessions").createIndex({ sessionId: 1 }, { unique: true });
  await db.collection("sessions").createIndex({ lastMessageAt: -1 });
  await db.collection("messages").createIndex({ sessionId: 1 });
  await db.collection("messages").createIndex({ sessionId: 1, timestamp: 1 });
}

function isValidSessionId(sessionId) {
  return typeof sessionId === "string" && /^[A-Za-z0-9_-]{1,100}$/.test(sessionId);
}

function validateChatBody(body, requireSessionId = true) {
  if (requireSessionId && (!body || !isValidSessionId(String(body.sessionId || "").trim()))) {
    return "sessionId is required and must use 1-100 letters, numbers, underscores, or dashes";
  }
  if (typeof body.message !== "string" || body.message.trim() === "") {
    return "message is required and must be a non-empty string";
  }
  return null;
}

function hasRentPiTopic(message, session) {
  const text = message.toLowerCase();
  if (rentPiKeywords.some((keyword) => text.includes(keyword))) return true;

  const hasFollowUpShape =
    /\bwhat about\b/i.test(message) &&
    (extractDates(message).length > 0 || extractMonths(message).length > 0 || extractProductId(message));
  return Boolean(session.memory.lastIntent && hasFollowUpShape);
}

function extractDates(message) {
  return message.match(/\b\d{4}-\d{2}-\d{2}\b/g) || [];
}

function extractMonths(message) {
  return message.match(/\b\d{4}-(0[1-9]|1[0-2])\b(?!-\d{2})/g) || [];
}

function extractProductId(message) {
  const match = message.match(/\bproduct\s+#?(\d+)\b/i);
  return match ? Number(match[1]) : null;
}

function extractUserId(message) {
  const match = message.match(/\buser\s+#?(\d+)\b/i);
  return match ? Number(match[1]) : null;
}

function getTodayDate() {
  return new Date().toISOString().slice(0, 10);
}

function getDiscountPercent(securityScore) {
  if (securityScore >= 80) return 20;
  if (securityScore >= 60) return 15;
  if (securityScore >= 40) return 10;
  if (securityScore >= 20) return 5;
  return 0;
}

function buildMemoryFromMessages(messages) {
  const memory = {};
  for (const message of messages) {
    if (!message || message.role !== "user") continue;
    const content = String(message.content || "");
    const dates = extractDates(content);
    const months = extractMonths(content);
    const productId = extractProductId(content);
    const userId = extractUserId(content);
    const text = content.toLowerCase();

    if (productId) memory.lastProductId = productId;
    if (userId) memory.lastUserId = userId;
    if (dates[0]) memory.lastDate = dates[0];
    if (dates[1]) {
      memory.lastFromDate = dates[0];
      memory.lastToDate = dates[1];
    }
    if (months[0]) memory.lastMonth = months[0];
    if (/available|availability/.test(text)) memory.lastIntent = "product_availability";
    if (/recommend|recommendation|trending|trend/.test(text)) {
      memory.lastIntent = "seasonal_recommendations";
      if (dates[0]) memory.lastRecommendationDate = dates[0];
    }
    if (/peak|biggest|boom/.test(text)) memory.lastIntent = "peak_rental_period";
    if (/surge/.test(text)) memory.lastIntent = "surge_days";
    if (/discount|security score|security/.test(text)) memory.lastIntent = "user_discount";
    if ((/most|top|highest|led/.test(text) && /categor/.test(text)) || /most rented category/.test(text)) {
      memory.lastIntent = "most_rented_category";
    }
  }
  return memory;
}

function resolveIntent(message, memory) {
  const text = message.toLowerCase();
  const dates = extractDates(message);
  const months = extractMonths(message);
  const productId = extractProductId(message) || memory.lastProductId || null;
  const userId = extractUserId(message) || memory.lastUserId || null;

  if ((/most|top|highest|led/.test(text) && /categor/.test(text)) || /most rented category/.test(text)) {
    return { name: "most_rented_category" };
  }

  if (/available|availability/.test(text) || (memory.lastIntent === "product_availability" && dates.length >= 2)) {
    if (!productId) return { name: "missing_info", reply: "Which product ID should I check availability for?" };
    if (dates.length < 2) return { name: "missing_info", reply: "Please provide the from and to dates in YYYY-MM-DD format." };
    return { name: "product_availability", productId, from: dates[0], to: dates[1] };
  }

  if (/recommend|recommendation|trending|trend/.test(text)) {
    return { name: "seasonal_recommendations", date: dates[0] || memory.lastRecommendationDate || getTodayDate() };
  }

  if (/peak|biggest|boom/.test(text)) {
    if (months.length < 2) return { name: "missing_info", reply: "Please provide from and to months in YYYY-MM format." };
    return { name: "peak_rental_period", from: months[0], to: months[1] };
  }

  if (/surge/.test(text)) {
    const month = months[0] || memory.lastMonth;
    if (!month) return { name: "missing_info", reply: "Which month should I check? Please use YYYY-MM format." };
    return { name: "surge_days", month };
  }

  if (/discount|security score|security/.test(text)) {
    if (!userId) return { name: "missing_info", reply: "Which user ID should I check?" };
    return { name: "user_discount", userId };
  }

  return {
    name: "fallback",
    reply: "I can help with category stats, availability, recommendations, peak windows, surge days, and user discounts. Which one do you want to check?"
  };
}

async function fetchJson(url, options = {}) {
  try {
    const response = await fetch(url, {
      ...options,
      headers: {
        accept: "application/json",
        ...(options.headers || {})
      },
      signal: AbortSignal.timeout(10000)
    });
    const text = await response.text();
    let data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = { message: text };
    }

    if (!response.ok) {
      const error = new Error(data && (data.message || data.error) ? data.message || data.error : "Data request failed");
      error.status = response.status;
      error.data = data;
      throw error;
    }

    return data;
  } catch (error) {
    if (error.status) throw error;
    const wrapped = new Error("Data service could not be reached");
    wrapped.status = 503;
    wrapped.data = { message: error.message };
    throw wrapped;
  }
}

function centralHeaders() {
  if (!CENTRAL_API_TOKEN || CENTRAL_API_TOKEN === "your_team_token_here") {
    const error = new Error("RentPi data configuration is unavailable.");
    error.status = 500;
    throw error;
  }
  return { authorization: `Bearer ${CENTRAL_API_TOKEN}` };
}

async function fetchGrounding(intent) {
  if (intent.name === "most_rented_category") {
    const data = await fetchJson(`${CENTRAL_API_URL}/api/data/rentals/stats?group_by=category`, {
      headers: centralHeaders()
    });
    const top = Array.isArray(data.data)
      ? data.data
          .map((item) => ({
            category: item && item.category,
            rental_count: Number(item && (item.rental_count ?? item.count))
          }))
          .filter((item) => item.category && Number.isFinite(item.rental_count))
          .sort((a, b) => b.rental_count - a.rental_count)[0]
      : null;
    if (!top) return null;
    return { intent: intent.name, data: top };
  }

  if (intent.name === "product_availability") {
    const data = await fetchJson(
      `${RENTAL_SERVICE_URL}/rentals/products/${intent.productId}/availability?from=${intent.from}&to=${intent.to}`
    );
    return { intent: intent.name, data };
  }

  if (intent.name === "seasonal_recommendations") {
    const data = await fetchJson(
      `${ANALYTICS_SERVICE_URL}/analytics/recommendations?date=${intent.date}&limit=5`
    );
    return { intent: intent.name, data };
  }

  if (intent.name === "peak_rental_period") {
    const data = await fetchJson(
      `${ANALYTICS_SERVICE_URL}/analytics/peak-window?from=${intent.from}&to=${intent.to}`
    );
    return { intent: intent.name, data };
  }

  if (intent.name === "surge_days") {
    const data = await fetchJson(`${ANALYTICS_SERVICE_URL}/analytics/surge-days?month=${intent.month}`);
    return { intent: intent.name, data };
  }

  if (intent.name === "user_discount") {
    const data = await fetchJson(`${CENTRAL_API_URL}/api/data/users/${intent.userId}`, {
      headers: centralHeaders()
    });
    const securityScore = Number(data && data.securityScore);
    if (!Number.isFinite(securityScore)) return null;
    return {
      intent: intent.name,
      data: {
        userId: intent.userId,
        securityScore,
        discountPercent: getDiscountPercent(securityScore)
      }
    };
  }

  return null;
}

function deterministicReply(grounding) {
  const { intent, data } = grounding;

  if (intent === "most_rented_category") {
    return `${data.category} led with ${data.rental_count.toLocaleString("en-US")} rentals.`;
  }

  if (intent === "product_availability") {
    if (typeof data.available !== "boolean") return "The availability data is unavailable, so I cannot answer that reliably.";
    return data.available
      ? `Product ${data.productId} is available from ${data.from} to ${data.to}.`
      : `Product ${data.productId} is not available from ${data.from} to ${data.to}.`;
  }

  if (intent === "seasonal_recommendations") {
    const recs = Array.isArray(data.recommendations) ? data.recommendations : [];
    if (recs.length === 0) return `I found no seasonal recommendations for ${data.date}.`;
    return `Top recommendations for ${data.date}: ${recs
      .map((item) => `${item.name} (${item.category}, score ${item.score})`)
      .join("; ")}.`;
  }

  if (intent === "peak_rental_period") {
    const peak = data.peakWindow;
    if (!peak) return "The peak rental window data is unavailable, so I cannot answer that reliably.";
    return `The peak rental window was ${peak.from} to ${peak.to}, with ${Number(peak.totalRentals).toLocaleString("en-US")} rentals.`;
  }

  if (intent === "surge_days") {
    const surge = Array.isArray(data.data) ? data.data.filter((day) => day.nextSurgeDate).slice(0, 5) : [];
    if (surge.length === 0) return `I found no next surge days for ${data.month}.`;
    return `For ${data.month}, next surge examples are: ${surge
      .map((day) => `${day.date} -> ${day.nextSurgeDate} (${day.daysUntil} days)`)
      .join("; ")}.`;
  }

  if (intent === "user_discount") {
    if (Number.isFinite(Number(data.discountPercent))) {
      return `User ${data.userId} has security score ${data.securityScore} and gets a ${data.discountPercent}% discount.`;
    }
    return `User ${data.userId} has security score ${data.securityScore}. The discount formula is unavailable.`;
  }

  return "I could not fetch the required RentPi data right now, so I cannot answer that reliably.";
}

function toLlmHistory(messages) {
  return messages.slice(-20).map((message) => ({
    role: message.role === "assistant" ? "model" : "user",
    parts: [{ text: String(message.content || "") }]
  }));
}

async function groundedLlmReply(message, grounding, historyMessages = []) {
  if (!GEMINI_API_KEY) return deterministicReply(grounding);

  const systemPrompt = [
    "You are RentPi Assistant.",
    "You answer questions only about RentPi rentals, products, categories, pricing, availability, users, discounts, trends, and recommendations.",
    "Use only the provided data context.",
    "Use conversation history for context, but use fetched data for factual RentPi numbers.",
    "Never invent numbers, product names, categories, availability, scores, dates, or discounts.",
    "If the data does not contain the answer, say that the information is unavailable.",
    "Keep the answer concise and helpful."
  ].join(" ");

  const userPrompt = JSON.stringify({
    originalUserMessage: message,
    detectedIntent: grounding.intent,
    fetchedData: grounding.data,
    instruction: "Answer only from this data."
  });

  try {
    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;
    const result = await fetchJson(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        systemInstruction: { parts: [{ text: systemPrompt }] },
        contents: [...toLlmHistory(historyMessages), { role: "user", parts: [{ text: userPrompt }] }]
      })
    });
    const reply = result && result.candidates && result.candidates[0]
      && result.candidates[0].content && result.candidates[0].content.parts
      && result.candidates[0].content.parts[0] && result.candidates[0].content.parts[0].text;
    return reply ? reply.trim() : deterministicReply(grounding);
  } catch {
    return deterministicReply(grounding);
  }
}

function fallbackSessionTitle(message) {
  const stopWords = new Set(["a", "an", "and", "are", "for", "from", "had", "has", "is", "of", "the", "to", "what", "which", "with"]);
  const words = String(message || "")
    .replace(/[^A-Za-z0-9 ]+/g, " ")
    .split(/\s+/)
    .filter(Boolean)
    .filter((word) => !stopWords.has(word.toLowerCase()))
    .slice(0, 5)
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase());
  return words.length > 0 ? words.join(" ") : "RentPi Chat";
}

function cleanSessionTitle(title, fallback) {
  const words = String(title || "")
    .replace(/["'`]/g, "")
    .replace(/[^\w\s-]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 5);
  return words.length > 0 ? words.join(" ") : fallback;
}

async function generateSessionName(firstMessage) {
  const fallback = fallbackSessionTitle(firstMessage);
  if (!GEMINI_API_KEY) return fallback;

  try {
    const url = `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;
    const result = await fetchJson(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        contents: [
          {
            role: "user",
            parts: [
              {
                text:
                  "Given this first user message, reply with ONLY a short 3-5 word title for this conversation. No punctuation.\n\n" +
                  firstMessage
              }
            ]
          }
        ]
      })
    });
    const title = result && result.candidates && result.candidates[0]
      && result.candidates[0].content && result.candidates[0].content.parts
      && result.candidates[0].content.parts[0] && result.candidates[0].content.parts[0].text;
    return cleanSessionTitle(title, fallback);
  } catch {
    return fallback;
  }
}

function dataErrorReply(error) {
  if (error.status === 429) return "RentPi data is rate-limited right now. Please try again later.";
  if (error.status >= 500) return "I could not fetch the required RentPi data right now, so I cannot answer that reliably.";
  if (error.status === 404) return "The requested RentPi data was not found.";
  return "I could not fetch the required RentPi data right now, so I cannot answer that reliably.";
}

function updateMemory(memory, intent) {
  if (!intent || intent.name === "missing_info" || intent.name === "fallback") return;
  memory.lastIntent = intent.name;
  if (intent.productId) memory.lastProductId = intent.productId;
  if (intent.userId) memory.lastUserId = intent.userId;
  if (intent.from) memory.lastFromDate = intent.from;
  if (intent.to) memory.lastToDate = intent.to;
  if (intent.month) memory.lastMonth = intent.month;
  if (intent.date) {
    memory.lastDate = intent.date;
    memory.lastRecommendationDate = intent.date;
  }
}

function serializeMessage(message) {
  return {
    role: message.role,
    content: message.content,
    timestamp: message.timestamp instanceof Date ? message.timestamp.toISOString() : new Date(message.timestamp).toISOString()
  };
}

async function listChatSessions(req, res) {
  const db = await connectMongo();
  const docs = await db
    .collection("sessions")
    .find({}, { projection: { _id: 0, sessionId: 1, name: 1, lastMessageAt: 1 } })
    .sort({ lastMessageAt: -1 })
    .toArray();

  return sendJson(res, 200, {
    sessions: docs.map((session) => ({
      sessionId: session.sessionId,
      name: session.name,
      lastMessageAt: session.lastMessageAt instanceof Date
        ? session.lastMessageAt.toISOString()
        : new Date(session.lastMessageAt).toISOString()
    }))
  });
}

async function getChatHistory(req, res, sessionId) {
  if (!isValidSessionId(sessionId)) {
    return sendJson(res, 400, { error: "Invalid request", message: "Invalid sessionId" });
  }

  const db = await connectMongo();
  const session = await db.collection("sessions").findOne({ sessionId }, { projection: { _id: 0 } });
  if (!session) return sendJson(res, 404, { error: "Session not found" });

  const messages = await db
    .collection("messages")
    .find({ sessionId }, { projection: { _id: 0, role: 1, content: 1, timestamp: 1 } })
    .sort({ timestamp: 1 })
    .toArray();

  return sendJson(res, 200, {
    sessionId,
    name: session.name,
    messages: messages.map(serializeMessage)
  });
}

async function deleteChatSession(req, res, sessionId) {
  if (!isValidSessionId(sessionId)) {
    return sendJson(res, 400, { error: "Invalid request", message: "Invalid sessionId" });
  }

  const db = await connectMongo();
  const session = await db.collection("sessions").findOne({ sessionId }, { projection: { _id: 1 } });
  if (!session) return sendJson(res, 404, { error: "Session not found" });

  await db.collection("messages").deleteMany({ sessionId });
  await db.collection("sessions").deleteOne({ sessionId });
  return sendJson(res, 200, { deleted: true, sessionId });
}

async function handleChat(req, res, includeMessages = false, requireSessionId = true) {
  const body = await readJson(req);
  const validationError = validateChatBody(body, requireSessionId);
  if (validationError) return sendJson(res, 400, { error: "Invalid request", message: validationError });

  const sessionId =
    isValidSessionId(String(body.sessionId || "").trim())
      ? body.sessionId.trim()
      : crypto.randomUUID();
  const message = body.message.trim();
  const db = await connectMongo();
  const existingSession = await db.collection("sessions").findOne({ sessionId });
  const priorMessages = await db
    .collection("messages")
    .find({ sessionId }, { projection: { _id: 0, role: 1, content: 1, timestamp: 1 } })
    .sort({ timestamp: 1 })
    .toArray();
  const memory = buildMemoryFromMessages(priorMessages);
  const historyForPrompt = priorMessages.map(serializeMessage);

  // Topic guard: this is before any Central API, internal service, or LLM call.
  let reply;
  let intent = null;
  if (!hasRentPiTopic(message, { memory })) {
    reply = "I can only help with RentPi rentals, products, categories, pricing, availability, users, discounts, trends, and recommendations.";
  } else {
    intent = resolveIntent(message, memory);
    if (intent.name === "missing_info" || intent.name === "fallback") {
      reply = intent.reply;
    } else {
      try {
        const grounding = await fetchGrounding(intent);
        if (!grounding || !grounding.data) {
          reply = "I could not fetch the required RentPi data right now, so I cannot answer that reliably.";
        } else {
          reply = await groundedLlmReply(message, grounding, historyForPrompt);
          updateMemory(memory, intent);
        }
      } catch (error) {
        reply = dataErrorReply(error);
      }
    }
  }

  const isNewSession = !existingSession;
  const userTimestamp = new Date();
  const assistantTimestamp = new Date(userTimestamp.getTime() + 1);
  const sessionName = isNewSession ? await generateSessionName(message) : existingSession.name;

  await db.collection("messages").insertMany([
    { sessionId, role: "user", content: message, timestamp: userTimestamp },
    { sessionId, role: "assistant", content: reply, timestamp: assistantTimestamp }
  ]);

  await db.collection("sessions").updateOne(
    { sessionId },
    {
      $setOnInsert: { sessionId, name: sessionName, createdAt: userTimestamp },
      $set: { lastMessageAt: assistantTimestamp }
    },
    { upsert: true }
  );

  if (includeMessages) {
    const messages = await db
      .collection("messages")
      .find({ sessionId }, { projection: { _id: 0, role: 1, content: 1, timestamp: 1 } })
      .sort({ timestamp: 1 })
      .toArray();
    return sendJson(res, 200, { sessionId, reply, messages: messages.map(serializeMessage) });
  }

  return sendJson(res, 200, { sessionId, reply });
}

async function routeRequest(req, res) {
  try {
    const path = req.url.split("?")[0];
    const historyMatch = path.match(/^\/chat\/([^/]+)\/history$/);
    const deleteMatch = path.match(/^\/chat\/([^/]+)$/);

    if (req.method === "GET" && path === "/status") {
      return sendJson(res, 200, { service: "agentic-service", status: "OK" });
    }
    if (req.method === "GET" && path === "/chat/sessions") {
      return listChatSessions(req, res);
    }
    if (req.method === "GET" && historyMatch) {
      return getChatHistory(req, res, historyMatch[1]);
    }
    if (req.method === "DELETE" && deleteMatch && deleteMatch[1] !== "sessions") {
      return deleteChatSession(req, res, deleteMatch[1]);
    }
    if (req.method === "POST" && path === "/chat") {
      return handleChat(req, res);
    }
    if (req.method === "POST" && path === "/chat/message") {
      return handleChat(req, res, true, false);
    }
    return sendJson(res, 404, { error: "Route not found" });
  } catch (error) {
    if (error instanceof SyntaxError) return sendJson(res, 400, { error: "Invalid JSON" });
    return sendJson(res, 500, { error: "Chat persistence failed", message: "The chat service could not complete the request." });
  }
}

connectMongo()
  .then(() => {
    http
      .createServer(routeRequest)
      .listen(PORT, () => console.log(`agentic-service listening on ${PORT}`));
  })
  .catch((error) => {
    console.error("Failed to initialize MongoDB:", error.message);
    process.exit(1);
  });
