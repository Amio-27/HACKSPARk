const http = require("http");

const PORT = Number(process.env.PORT || 8003);
const CENTRAL_API_URL = (process.env.CENTRAL_API_URL || "https://technocracy.brittoo.xyz").replace(/\/$/, "");
const CENTRAL_API_TOKEN = process.env.CENTRAL_API_TOKEN || "";
const DAY_MS = 86400000;
const WINDOW_DAYS = 7;

function sendJson(res, status, body) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

function isValidMonth(value) {
  return /^\d{4}-(0[1-9]|1[0-2])$/.test(value || "");
}

function parseMonthToParts(value) {
  const [year, month] = value.split("-").map(Number);
  return { year, month };
}

function getUtcDayNumber(year, monthIndex, day) {
  const date = new Date(0);
  date.setUTCFullYear(year, monthIndex, day);
  date.setUTCHours(0, 0, 0, 0);
  return Math.floor(date.getTime() / DAY_MS);
}

function monthIndex(value) {
  const { year, month } = parseMonthToParts(value);
  return year * 12 + month - 1;
}

function getMonthStartDay(value) {
  const { year, month } = parseMonthToParts(value);
  return getUtcDayNumber(year, month - 1, 1);
}

function getMonthEndDay(value) {
  const { year, month } = parseMonthToParts(value);
  return getUtcDayNumber(year, month, 0);
}

function generateMonthsBetween(from, to) {
  const months = [];
  const fromIndex = monthIndex(from);
  const toIndex = monthIndex(to);

  for (let index = fromIndex; index <= toIndex; index += 1) {
    const year = Math.floor(index / 12);
    const month = (index % 12) + 1;
    months.push(`${String(year).padStart(4, "0")}-${String(month).padStart(2, "0")}`);
  }

  return months;
}

function getTotalDaysInclusive(startDay, endDay) {
  return endDay - startDay + 1;
}

function parseDateToDayNumber(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value || "")) return null;
  const [year, month, day] = value.split("-").map(Number);
  const candidate = getUtcDayNumber(year, month - 1, day);
  if (formatDayNumber(candidate) !== value) return null;
  return candidate;
}

function formatDayNumber(dayNumber) {
  return new Date(dayNumber * DAY_MS).toISOString().slice(0, 10);
}

function parseUpstreamBody(body, contentType) {
  const text = body.toString("utf8");
  if (!text) return null;
  if (!contentType.includes("application/json")) return { message: text };

  try {
    return JSON.parse(text);
  } catch {
    return { message: text };
  }
}

function centralErrorMessage(status) {
  if (status === 429) return "Central API rate limit exceeded";
  if (status >= 500 && status < 600) return "Central API error";
  if (status === 401 || status === 403) return "Central API authentication failed";
  return "Central API rejected the request";
}

async function fetchMonthlyStats(month) {
  if (!CENTRAL_API_TOKEN || CENTRAL_API_TOKEN === "your_team_token_here") {
    const error = new Error("Central API token is not configured");
    error.status = 500;
    throw error;
  }

  const path = `/api/data/rentals/stats?group_by=date&month=${encodeURIComponent(month)}`;

  try {
    const response = await fetch(`${CENTRAL_API_URL}${path}`, {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${CENTRAL_API_TOKEN}`
      },
      signal: AbortSignal.timeout(10000)
    });
    const contentType = response.headers.get("content-type") || "application/json";
    const body = Buffer.from(await response.arrayBuffer());
    const parsed = parseUpstreamBody(body, contentType);

    if (!response.ok) {
      const error = new Error(centralErrorMessage(response.status));
      error.status = response.status;
      error.upstream = parsed;
      throw error;
    }
    if (!parsed || !Array.isArray(parsed.data)) {
      const error = new Error("Central API returned an invalid rental stats response");
      error.status = 502;
      error.upstream = parsed;
      throw error;
    }

    return parsed;
  } catch (error) {
    if (error.status) throw error;
    const upstreamError = new Error("Central API unavailable");
    upstreamError.status = 502;
    upstreamError.upstream = { message: error.message };
    throw upstreamError;
  }
}

function normalizeStatsToFullRange(statsResponses, startDay, endDay) {
  const totalDays = getTotalDaysInclusive(startDay, endDay);
  const fullRangeCounts = new Array(totalDays).fill(0);

  for (const response of statsResponses) {
    for (const stat of response.data) {
      if (!stat || !stat.date) continue;
      const statDay = parseDateToDayNumber(String(stat.date).slice(0, 10));
      if (statDay === null) continue;
      const index = statDay - startDay;

      if (index >= 0 && index < fullRangeCounts.length) {
        fullRangeCounts[index] += Number(stat.count || 0);
      }
    }
  }

  return fullRangeCounts;
}

function findPeakSevenDayWindow(counts) {
  let currentSum = 0;

  for (let index = 0; index < WINDOW_DAYS; index += 1) {
    currentSum += counts[index];
  }

  let maxSum = currentSum;
  let peakStartIndex = 0;

  for (let index = WINDOW_DAYS; index < counts.length; index += 1) {
    currentSum = currentSum + counts[index] - counts[index - WINDOW_DAYS];

    if (currentSum > maxSum) {
      maxSum = currentSum;
      peakStartIndex = index - WINDOW_DAYS + 1;
    }
  }

  return { peakStartIndex, totalRentals: maxSum };
}

function normalizeStatsToDailyData(statsResponse, startDay, endDay) {
  const counts = normalizeStatsToFullRange([statsResponse], startDay, endDay);

  return counts.map((count, index) => ({
    date: formatDayNumber(startDay + index),
    count
  }));
}

function findNextSurgeDays(dailyData) {
  const result = dailyData.map((day) => ({
    date: day.date,
    count: day.count,
    nextSurgeDate: null,
    daysUntil: null
  }));
  const stack = [];

  for (let index = 0; index < dailyData.length; index += 1) {
    while (
      stack.length > 0 &&
      dailyData[index].count > dailyData[stack[stack.length - 1]].count
    ) {
      const previousIndex = stack.pop();
      result[previousIndex].nextSurgeDate = dailyData[index].date;
      result[previousIndex].daysUntil = index - previousIndex;
    }

    stack.push(index);
  }

  return result;
}

function peakWindowErrorMessage(status, error) {
  if (status === 500) return "Central API token is not configured";
  if (status === 429) return "Central API rate limit exceeded";
  if (status === 502) return error.message || "Central API unavailable";
  if (status >= 500 && status < 600) return "Central API error";
  return error.message || "Failed to calculate peak rental window";
}

async function handlePeakWindowRequest(req, res) {
  const sourceUrl = new URL(req.url, `http://${req.headers.host}`);
  const from = sourceUrl.searchParams.get("from");
  const to = sourceUrl.searchParams.get("to");

  if (!from) {
    return sendJson(res, 400, {
      error: "Peak window calculation failed",
      message: "from is required"
    });
  }
  if (!to) {
    return sendJson(res, 400, {
      error: "Peak window calculation failed",
      message: "to is required"
    });
  }
  if (!isValidMonth(from) || !isValidMonth(to)) {
    return sendJson(res, 400, {
      error: "Peak window calculation failed",
      message: "from and to must be valid YYYY-MM strings"
    });
  }
  if (monthIndex(from) > monthIndex(to)) {
    return sendJson(res, 400, {
      error: "Peak window calculation failed",
      message: "from must be less than or equal to to"
    });
  }

  const months = generateMonthsBetween(from, to);
  if (months.length > 12) {
    return sendJson(res, 400, {
      error: "Peak window calculation failed",
      message: "Date range must not exceed 12 months"
    });
  }

  const startDay = getMonthStartDay(from);
  const endDay = getMonthEndDay(to);
  const totalDays = getTotalDaysInclusive(startDay, endDay);

  if (totalDays < WINDOW_DAYS) {
    return sendJson(res, 400, {
      error: "Peak window calculation failed",
      message: "Date range must include at least 7 calendar days"
    });
  }

  try {
    const statsResponses = await Promise.all(months.map(fetchMonthlyStats));
    const fullRangeCounts = normalizeStatsToFullRange(statsResponses, startDay, endDay);
    const peak = findPeakSevenDayWindow(fullRangeCounts);
    const peakFrom = startDay + peak.peakStartIndex;

    return sendJson(res, 200, {
      from,
      to,
      peakWindow: {
        from: formatDayNumber(peakFrom),
        to: formatDayNumber(peakFrom + WINDOW_DAYS - 1),
        totalRentals: peak.totalRentals
      }
    });
  } catch (error) {
    const status = error.status || 502;
    return sendJson(res, status, {
      error: "Peak window calculation failed",
      message: peakWindowErrorMessage(status, error),
      details: error.upstream
    });
  }
}

function surgeDaysErrorMessage(status, error) {
  if (status === 500) return "Central API token is not configured";
  if (status === 429) return "Central API rate limit exceeded";
  if (status === 502) return error.message || "Central API unavailable";
  if (status >= 500 && status < 600) return "Central API error";
  return error.message || "Failed to calculate surge days";
}

async function handleSurgeDaysRequest(req, res) {
  const sourceUrl = new URL(req.url, `http://${req.headers.host}`);
  const month = sourceUrl.searchParams.get("month");

  if (!month) {
    return sendJson(res, 400, {
      error: "Surge days calculation failed",
      message: "month is required"
    });
  }
  if (!isValidMonth(month)) {
    return sendJson(res, 400, {
      error: "Surge days calculation failed",
      message: "month must be a valid YYYY-MM string"
    });
  }

  try {
    const statsResponse = await fetchMonthlyStats(month);
    const startDay = getMonthStartDay(month);
    const endDay = getMonthEndDay(month);
    const dailyData = normalizeStatsToDailyData(statsResponse, startDay, endDay);

    return sendJson(res, 200, {
      month,
      data: findNextSurgeDays(dailyData)
    });
  } catch (error) {
    const status = error.status || 502;
    return sendJson(res, status, {
      error: "Surge days calculation failed",
      message: surgeDaysErrorMessage(status, error),
      details: error.upstream
    });
  }
}

http
  .createServer((req, res) => {
    const path = req.url.split("?")[0];

    if (req.method === "GET" && path === "/status") {
      return sendJson(res, 200, { service: "analytics-service", status: "OK" });
    }
    if (req.method === "GET" && path === "/analytics/summary") {
      return sendJson(res, 200, {
        service: "analytics-service",
        message: "Analytics endpoints are ready to be extended."
      });
    }
    if (req.method === "GET" && path === "/analytics/peak-window") {
      return handlePeakWindowRequest(req, res);
    }
    if (req.method === "GET" && path === "/analytics/surge-days") {
      return handleSurgeDaysRequest(req, res);
    }
    sendJson(res, 404, { error: "Route not found" });
  })
  .listen(PORT, () => console.log(`analytics-service listening on ${PORT}`));
