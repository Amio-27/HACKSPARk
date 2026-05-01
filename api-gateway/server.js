const http = require("http");

const PORT = Number(process.env.PORT || 8000);

const services = {
  "user-service": process.env.USER_SERVICE_URL || "http://user-service:8001",
  "rental-service": process.env.RENTAL_SERVICE_URL || "http://rental-service:8002",
  "analytics-service": process.env.ANALYTICS_SERVICE_URL || "http://analytics-service:8003",
  "agentic-service": process.env.AGENTIC_SERVICE_URL || "http://agentic-service:8004"
};

function sendJson(res, status, body) {
  res.writeHead(status, {
    "access-control-allow-headers": "authorization, content-type",
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-origin": "*",
    "content-type": "application/json"
  });
  res.end(JSON.stringify(body));
}

async function readBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  return Buffer.concat(chunks);
}

async function serviceStatus(name, baseUrl) {
  try {
    const response = await fetch(`${baseUrl}/status`, { signal: AbortSignal.timeout(3000) });
    if (!response.ok) return [name, "UNREACHABLE"];
    const data = await response.json();
    return [name, data.status || "UNREACHABLE"];
  } catch {
    return [name, "UNREACHABLE"];
  }
}

async function proxy(req, res, baseUrl, stripPrefix) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const path = url.pathname.replace(stripPrefix, "") || "/";
  const target = `${baseUrl}${path}${url.search}`;
  const body = ["GET", "HEAD"].includes(req.method) ? undefined : await readBody(req);
  const headers = {};
  for (const name of ["authorization", "content-type", "accept"]) {
    if (req.headers[name]) headers[name] = req.headers[name];
  }

  try {
    const response = await fetch(target, { method: req.method, headers, body });
    const responseBody = Buffer.from(await response.arrayBuffer());
    res.writeHead(response.status, {
      ...Object.fromEntries(response.headers.entries()),
      "access-control-allow-origin": "*"
    });
    res.end(responseBody);
  } catch (error) {
    sendJson(res, 502, { error: "Downstream service unavailable", detail: error.message });
  }
}

http
  .createServer(async (req, res) => {
    if (req.method === "OPTIONS") {
      return sendJson(res, 204, {});
    }

    if (req.method === "GET" && req.url.split("?")[0] === "/status") {
      const downstreamEntries = await Promise.all(
        Object.entries(services).map(([name, baseUrl]) => serviceStatus(name, baseUrl))
      );
      return sendJson(res, 200, {
        service: "api-gateway",
        status: "OK",
        downstream: Object.fromEntries(downstreamEntries)
      });
    }

    if (req.url.startsWith("/users")) return proxy(req, res, services["user-service"], "");
    if (req.url.startsWith("/rentals")) return proxy(req, res, services["rental-service"], "");
    if (req.url.startsWith("/analytics")) return proxy(req, res, services["analytics-service"], "");
    if (req.url.startsWith("/chat")) return proxy(req, res, services["agentic-service"], "");

    sendJson(res, 404, { error: "Route not found" });
  })
  .listen(PORT, () => console.log(`api-gateway listening on ${PORT}`));
