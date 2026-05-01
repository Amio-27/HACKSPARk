const http = require("http");

const PORT = Number(process.env.PORT || 8003);
const CENTRAL_API_URL = (process.env.CENTRAL_API_URL || "https://technocracy.brittoo.xyz").replace(/\/$/, "");
const CENTRAL_API_TOKEN = process.env.CENTRAL_API_TOKEN || "";
const DAY_MS = 86400000;
const WINDOW_DAYS = 7;
const SEASONAL_RADIUS_DAYS = 7;
const HISTORICAL_YEARS = 2;
const RENTAL_PAGE_LIMIT = 100;
const MAX_RECOMMENDATION_LIMIT = 50;

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

function validateDateOnly(value) {
  if (!value) return "date is required";
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return "date must be a valid YYYY-MM-DD date";
  if (parseDateToDayNumber(value) === null) return "date must be a real calendar date";
  return null;
}

function validateRecommendationLimit(value) {
  if (!value) return "limit is required";
  if (!/^[1-9]\d*$/.test(value)) return "limit must be a positive integer";
  if (Number(value) > MAX_RECOMMENDATION_LIMIT) {
    return `limit must be less than or equal to ${MAX_RECOMMENDATION_LIMIT}`;
  }
  return null;
}

function parseDateUTC(value) {
  const [year, month, day] = value.split("-").map(Number);
  const date = new Date(0);
  date.setUTCFullYear(year, month - 1, day);
  date.setUTCHours(0, 0, 0, 0);
  return date;
}

function formatDateUTC(date) {
  return date.toISOString().slice(0, 10);
}

function addDaysUTC(date, days) {
  const result = new Date(date.getTime());
  result.setUTCDate(result.getUTCDate() + days);
  return result;
}

function getDaysInUtcMonth(year, monthIndex) {
  const date = new Date(0);
  date.setUTCFullYear(year, monthIndex + 1, 0);
  date.setUTCHours(0, 0, 0, 0);
  return date.getUTCDate();
}

function mapDateToHistoricalYear(date, year) {
  const monthIndex = date.getUTCMonth();
  let day = date.getUTCDate();

  // Feb 29 does not exist every year, so anchor non-leap historical windows on Feb 28.
  if (monthIndex === 1 && day === 29 && getDaysInUtcMonth(year, monthIndex) < 29) {
    day = 28;
  }

  const result = new Date(0);
  result.setUTCFullYear(year, monthIndex, day);
  result.setUTCHours(0, 0, 0, 0);
  return result;
}

function buildHistoricalSeasonalWindows(date) {
  const windows = [];
  const sourceYear = date.getUTCFullYear();

  for (let yearsAgo = 1; yearsAgo <= HISTORICAL_YEARS; yearsAgo += 1) {
    const baseDate = mapDateToHistoricalYear(date, sourceYear - yearsAgo);
    const fromDate = addDaysUTC(baseDate, -SEASONAL_RADIUS_DAYS);
    const toDate = addDaysUTC(baseDate, SEASONAL_RADIUS_DAYS);
    const from = formatDateUTC(fromDate);
    const to = formatDateUTC(toDate);

    windows.push({
      from,
      to,
      startDay: parseDateToDayNumber(from),
      endDay: parseDateToDayNumber(to)
    });
  }

  return windows;
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

function sanitizeUpstreamDetails(value) {
  if (value === null || value === undefined) return value;
  if (typeof value === "string") {
    if (CENTRAL_API_TOKEN && CENTRAL_API_TOKEN !== "your_team_token_here") {
      return value.split(CENTRAL_API_TOKEN).join("[redacted]");
    }
    return value;
  }
  if (Array.isArray(value)) return value.map(sanitizeUpstreamDetails);
  if (typeof value !== "object") return value;

  const sanitized = {};
  for (const [key, nestedValue] of Object.entries(value)) {
    if (/authorization|token|secret|password/i.test(key)) {
      sanitized[key] = "[redacted]";
    } else {
      sanitized[key] = sanitizeUpstreamDetails(nestedValue);
    }
  }
  return sanitized;
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

async function fetchAllRentalsForWindow(from, to) {
  if (!CENTRAL_API_TOKEN || CENTRAL_API_TOKEN === "your_team_token_here") {
    const error = new Error("Central API token is not configured");
    error.status = 500;
    throw error;
  }

  const rentals = [];
  let page = 1;
  let totalPages = 1;

  do {
    const path =
      `/api/data/rentals?from=${encodeURIComponent(from)}` +
      `&to=${encodeURIComponent(to)}&page=${page}&limit=${RENTAL_PAGE_LIMIT}`;

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
        const error = new Error("Central API returned an invalid rentals response");
        error.status = 502;
        error.upstream = parsed;
        throw error;
      }

      rentals.push(...parsed.data);
      const responseLimit = Number(parsed.limit || RENTAL_PAGE_LIMIT);
      const totalPagesFromTotal =
        parsed.total && responseLimit > 0 ? Math.ceil(Number(parsed.total) / responseLimit) : null;
      const parsedTotalPages = Number(parsed.totalPages || parsed.total_pages || totalPagesFromTotal || page);
      totalPages = Number.isFinite(parsedTotalPages) && parsedTotalPages > 0 ? parsedTotalPages : page;
      page += 1;
    } catch (error) {
      if (error.status) throw error;
      const upstreamError = new Error("Central API unavailable");
      upstreamError.status = 502;
      upstreamError.upstream = { message: error.message };
      throw upstreamError;
    }
  } while (page <= totalPages);

  return rentals;
}

function isRentalStartInsideWindows(rental, windows) {
  const rentalStart = parseDateToDayNumber(String(rental.rentalStart || "").slice(0, 10));
  if (rentalStart === null) return false;
  return windows.some((window) => rentalStart >= window.startDay && rentalStart <= window.endDay);
}

function scoreRentalsByProduct(rentals, windows) {
  const scoreMap = new Map();

  for (const rental of rentals) {
    if (!rental || !isRentalStartInsideWindows(rental, windows)) continue;

    const productId = Number(rental.productId);
    if (!Number.isSafeInteger(productId) || productId <= 0) continue;

    scoreMap.set(productId, (scoreMap.get(productId) || 0) + 1);
  }

  return scoreMap;
}

function selectTopKProducts(scoreMap, limit) {
  return Array.from(scoreMap, ([productId, score]) => ({ productId, score }))
    .sort((a, b) => b.score - a.score || a.productId - b.productId)
    .slice(0, limit);
}

async function fetchProductsBatch(productIds) {
  if (productIds.length === 0) return [];
  if (!CENTRAL_API_TOKEN || CENTRAL_API_TOKEN === "your_team_token_here") {
    const error = new Error("Central API token is not configured");
    error.status = 500;
    throw error;
  }

  const path = `/api/data/products/batch?ids=${productIds.map(encodeURIComponent).join(",")}`;

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
      const error = new Error("Central API returned an invalid products batch response");
      error.status = 502;
      error.upstream = parsed;
      throw error;
    }

    return parsed.data;
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

function recommendationsErrorMessage(status, error) {
  if (status === 500 && error.message === "Central API token is not configured") {
    return "Central API token is not configured";
  }
  if (status === 429) return "Central API rate limit exceeded";
  if (status === 502) return error.message || "Central API unavailable";
  if (status >= 500 && status < 600) return "Central API error";
  return error.message || "Failed to calculate seasonal recommendations";
}

async function handleRecommendationsRequest(req, res) {
  const sourceUrl = new URL(req.url, `http://${req.headers.host}`);
  const dateText = sourceUrl.searchParams.get("date");
  const limitText = sourceUrl.searchParams.get("limit");
  const dateError = validateDateOnly(dateText);
  const limitError = validateRecommendationLimit(limitText);

  if (dateError) {
    return sendJson(res, 400, {
      error: "Seasonal recommendations calculation failed",
      message: dateError
    });
  }
  if (limitError) {
    return sendJson(res, 400, {
      error: "Seasonal recommendations calculation failed",
      message: limitError
    });
  }

  try {
    const limit = Number(limitText);
    const windows = buildHistoricalSeasonalWindows(parseDateUTC(dateText));
    const rentalsByWindow = await Promise.all(
      windows.map((window) => fetchAllRentalsForWindow(window.from, window.to))
    );
    const scoreMap = scoreRentalsByProduct(rentalsByWindow.flat(), windows);

    if (scoreMap.size === 0) {
      return sendJson(res, 200, {
        date: dateText,
        recommendations: []
      });
    }

    const topProducts = selectTopKProducts(scoreMap, limit);
    const productIds = topProducts.map((product) => product.productId);
    const products = await fetchProductsBatch(productIds);
    const productsById = new Map();

    for (const product of products) {
      const productId = Number(product && (product.id ?? product.productId));
      if (Number.isSafeInteger(productId)) productsById.set(productId, product);
    }

    const recommendations = topProducts
      .map((topProduct) => {
        const product = productsById.get(topProduct.productId);
        if (!product) return null;
        return {
          productId: topProduct.productId,
          name: product.name,
          category: product.category,
          score: topProduct.score
        };
      })
      .filter(Boolean)
      .sort((a, b) => b.score - a.score || a.productId - b.productId);

    return sendJson(res, 200, {
      date: dateText,
      recommendations
    });
  } catch (error) {
    const status = error.status || 502;
    return sendJson(res, status, {
      error: "Seasonal recommendations calculation failed",
      message: recommendationsErrorMessage(status, error),
      details: sanitizeUpstreamDetails(error.upstream)
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
    if (req.method === "GET" && path === "/analytics/recommendations") {
      return handleRecommendationsRequest(req, res);
    }
    sendJson(res, 404, { error: "Route not found" });
  })
  .listen(PORT, () => console.log(`analytics-service listening on ${PORT}`));
