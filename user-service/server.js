const crypto = require("crypto");
const http = require("http");
const { Pool } = require("pg");

const PORT = Number(process.env.PORT || 8001);
const JWT_SECRET = process.env.JWT_SECRET || "local-dev-secret";
const CENTRAL_API_URL = (process.env.CENTRAL_API_URL || "https://technocracy.brittoo.xyz").replace(/\/$/, "");
const CENTRAL_API_TOKEN = process.env.CENTRAL_API_TOKEN || "";
const CENTRAL_USER_CACHE_TTL_MS = 60 * 1000;
const centralUserCache = new Map();
const pool = new Pool({
  connectionString: process.env.DATABASE_URL
});

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

function base64url(input) {
  return Buffer.from(input).toString("base64url");
}

function signJwt(payload) {
  const header = base64url(JSON.stringify({ alg: "HS256", typ: "JWT" }));
  const body = base64url(JSON.stringify({ ...payload, exp: Math.floor(Date.now() / 1000) + 86400 }));
  const signature = crypto.createHmac("sha256", JWT_SECRET).update(`${header}.${body}`).digest("base64url");
  return `${header}.${body}.${signature}`;
}

function verifyJwt(token) {
  const [header, body, signature] = token.split(".");
  if (!header || !body || !signature) return null;
  const expected = crypto
    .createHmac("sha256", JWT_SECRET)
    .update(`${header}.${body}`)
    .digest("base64url");

  // Buffer বানিয়ে compare করো
  const sigBuf = Buffer.from(signature);
  const expBuf = Buffer.from(expected);
  if (sigBuf.length !== expBuf.length) return null;  // ← fix
  if (!crypto.timingSafeEqual(sigBuf, expBuf)) return null;

  const payload = JSON.parse(Buffer.from(body, "base64url").toString("utf8"));
  if (payload.exp && payload.exp < Math.floor(Date.now() / 1000)) return null;
  return payload;
}

function hashPassword(password, salt = crypto.randomBytes(16).toString("hex")) {
  const hash = crypto.pbkdf2Sync(password, salt, 120000, 32, "sha256").toString("hex");
  return `${salt}:${hash}`;
}

function verifyPassword(password, stored) {
  const [salt] = stored.split(":");
  return hashPassword(password, salt) === stored;
}

function getDiscountPercent(securityScore) {
  if (securityScore >= 80) return 20;
  if (securityScore >= 60) return 15;
  if (securityScore >= 40) return 10;
  if (securityScore >= 20) return 5;
  return 0;
}

async function fetchCentralUser(userId) {
  if (!CENTRAL_API_TOKEN || CENTRAL_API_TOKEN === "your_team_token_here") {
    const error = new Error("Central API token is not configured");
    error.status = 500;
    throw error;
  }

  const cached = centralUserCache.get(userId);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.user;
  }

  const response = await fetch(`${CENTRAL_API_URL}/api/data/users/${userId}`, {
    headers: {
      accept: "application/json",
      authorization: `Bearer ${CENTRAL_API_TOKEN}`
    },
    signal: AbortSignal.timeout(4000)
  });

  if (response.status === 404) {
    const error = new Error("User not found");
    error.status = 404;
    throw error;
  }

  if (response.status === 429) {
    const error = new Error("Central API rate limit exceeded");
    error.status = 429;
    throw error;
  }

  if (!response.ok) {
    const error = new Error("Failed to fetch user from Central API");
    error.status = 502;
    throw error;
  }

  const user = await response.json();
  centralUserCache.set(userId, {
    user,
    expiresAt: Date.now() + CENTRAL_USER_CACHE_TTL_MS
  });
  return user;
}

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      security_score INTEGER NOT NULL DEFAULT 50,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

function publicUser(row) {
  return {
    id: row.id,
    name: row.name,
    email: row.email,
    securityScore: row.security_score
  };
}

function userToken(row) {
  return signJwt({
    sub: row.id,
    name: row.name,
    email: row.email,
    securityScore: row.security_score
  });
}

const server = http.createServer(async (req, res) => {
    try {
      const path = req.url.split("?")[0];

      if (req.method === "GET" && req.url === "/status") {
        return sendJson(res, 200, { service: "user-service", status: "OK" });
      }

      if (req.method === "POST" && req.url === "/users/register") {
        const { name, email, password } = await readJson(req);
        if (!name || !email || !password) return sendJson(res, 400, { error: "name, email, and password are required" });
        const normalizedEmail = String(email).toLowerCase();
        const existing = await pool.query("SELECT id FROM users WHERE email = $1", [normalizedEmail]);
        if (existing.rowCount > 0) return sendJson(res, 409, { error: "Email already registered" });

        const result = await pool.query(
          "INSERT INTO users (name, email, password_hash) VALUES ($1, $2, $3) RETURNING id, name, email, security_score",
          [name, normalizedEmail, hashPassword(password)]
        );
        const user = result.rows[0];
        return sendJson(res, 201, { token: userToken(user), user: publicUser(user) });
      }

      if (req.method === "POST" && req.url === "/users/login") {
        const { email, password } = await readJson(req);
        const result = await pool.query(
          "SELECT id, name, email, password_hash, security_score FROM users WHERE email = $1",
          [String(email || "").toLowerCase()]
        );
        const user = result.rows[0];
        if (!user || !verifyPassword(password || "", user.password_hash)) {
          return sendJson(res, 401, { error: "Invalid credentials" });
        }
        return sendJson(res, 200, { token: userToken(user), user: publicUser(user) });
      }

      if (req.method === "GET" && req.url === "/users/me") {
        const token = (req.headers.authorization || "").replace(/^Bearer\s+/i, "");
        const payload = token ? verifyJwt(token) : null;
        if (!payload) return sendJson(res, 401, { error: "Missing or invalid token" });
        return sendJson(res, 200, { user: { id: payload.sub, name: payload.name, email: payload.email, securityScore: payload.securityScore } });
      }

      const discountMatch = path.match(/^\/users\/([^/]+)\/discount$/);
      if (req.method === "GET" && discountMatch) {
        const userIdText = discountMatch[1];
        if (!/^[1-9]\d*$/.test(userIdText)) {
          return sendJson(res, 400, { error: "Invalid user id" });
        }

        try {
          const userId = Number(userIdText);
          const user = await fetchCentralUser(userId);
          const securityScore = Number(user.securityScore);
          return sendJson(res, 200, {
            userId,
            securityScore,
            discountPercent: getDiscountPercent(securityScore)
          });
        } catch (error) {
          if (error.status === 500) return sendJson(res, 500, { error: "Central API token is not configured" });
          if (error.status === 404) return sendJson(res, 404, { error: "User not found" });
          if (error.status === 429) return sendJson(res, 429, { error: "Central API rate limit exceeded" });
          return sendJson(res, 502, { error: "Failed to fetch user from Central API" });
        }
      }

      sendJson(res, 404, { error: "Route not found" });
    } catch (error) {
      sendJson(res, 500, { error: "user-service error", detail: error.message });
    }
  });

initDb()
  .then(() => server.listen(PORT, () => console.log(`user-service listening on ${PORT}`)))
  .catch((error) => {
    console.error("Failed to initialize database", error);
    process.exit(1);
  });
