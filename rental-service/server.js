const http = require("http");

const PORT = Number(process.env.PORT || 8002);
const CENTRAL_API_URL = (process.env.CENTRAL_API_URL || "https://technocracy.brittoo.xyz").replace(/\/$/, "");
const CENTRAL_API_TOKEN = process.env.CENTRAL_API_TOKEN || "";
const CATEGORY_CACHE_TTL_MS = 1000 * 60 * 60;

let categoryCache = {
  categories: null,
  expiresAt: 0,
  pending: null
};

function sendJson(res, status, body) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

async function proxyCentral(req, res, centralPath) {
  if (!CENTRAL_API_TOKEN || CENTRAL_API_TOKEN === "your_team_token_here") {
    return sendJson(res, 503, { error: "CENTRAL_API_TOKEN is not configured" });
  }

  const sourceUrl = new URL(req.url, `http://${req.headers.host}`);
  const target = `${CENTRAL_API_URL}${centralPath}${sourceUrl.search}`;

  try {
    const response = await fetch(target, {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${CENTRAL_API_TOKEN}`
      },
      signal: AbortSignal.timeout(10000)
    });
    const contentType = response.headers.get("content-type") || "application/json";
    const body = Buffer.from(await response.arrayBuffer());

    if (response.ok) {
      res.writeHead(response.status, { "content-type": contentType });
      return res.end(body);
    }

    const upstream = parseUpstreamBody(body, contentType);
    return sendJson(res, response.status, {
      error: centralErrorMessage(response.status),
      status: response.status,
      retryAfterSeconds: upstream?.retryAfterSeconds || response.headers.get("retry-after") || undefined,
      upstream
    });
  } catch (error) {
    const status = error.name === "TimeoutError" ? 504 : 502;
    sendJson(res, status, {
      error: status === 504 ? "Central API request timed out" : "Central API unavailable",
      detail: error.message
    });
  }
}

async function fetchCentralJson(path) {
  if (!CENTRAL_API_TOKEN || CENTRAL_API_TOKEN === "your_team_token_here") {
    const error = new Error("CENTRAL_API_TOKEN is not configured");
    error.status = 503;
    throw error;
  }

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

  return parsed;
}

function parseDateOnly(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value || "")) return null;
  const [year, month, day] = value.split("-").map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  if (
    date.getUTCFullYear() !== year ||
    date.getUTCMonth() !== month - 1 ||
    date.getUTCDate() !== day
  ) {
    return null;
  }
  return Math.floor(date.getTime() / 86400000);
}

function formatDateOnly(dayNumber) {
  return new Date(dayNumber * 86400000).toISOString().slice(0, 10);
}

function isValidMonth(value) {
  if (!/^\d{4}-\d{2}$/.test(value || "")) return false;
  const month = Number(value.slice(5, 7));
  return month >= 1 && month <= 12;
}

function parseMonth(value) {
  const [year, month] = value.split("-").map(Number);
  return { year, month };
}

function monthIndex(value) {
  const { year, month } = parseMonth(value);
  return year * 12 + month - 1;
}

function generateMonthsBetween(from, to) {
  const months = [];
  const fromIndex = monthIndex(from);
  const toIndex = monthIndex(to);

  for (let index = fromIndex; index <= toIndex; index += 1) {
    const year = Math.floor(index / 12);
    const month = (index % 12) + 1;
    months.push(`${year}-${String(month).padStart(2, "0")}`);
  }

  return months;
}

function aggregateRentalStats(responses) {
  const countsByDate = new Map();

  for (const response of responses) {
    if (!response || !Array.isArray(response.data)) {
      const error = new Error("Central API returned an invalid rental stats response");
      error.status = 502;
      error.upstream = response;
      throw error;
    }

    for (const record of response.data) {
      if (!record || !record.date) continue;
      const date = String(record.date).slice(0, 10);
      const count = Number(record.count || 0);
      countsByDate.set(date, (countsByDate.get(date) || 0) + count);
    }
  }

  return Array.from(countsByDate, ([date, count]) => ({ date, count }));
}

function ranksHigher(a, b) {
  if (a.count !== b.count) return a.count > b.count;
  return a.date < b.date;
}

class MinHeap {
  constructor(isLowerPriority) {
    this.items = [];
    this.isLowerPriority = isLowerPriority;
  }

  size() {
    return this.items.length;
  }

  peek() {
    return this.items[0];
  }

  push(item) {
    this.items.push(item);
    this.bubbleUp(this.items.length - 1);
  }

  pop() {
    if (this.items.length === 0) return undefined;
    const root = this.items[0];
    const last = this.items.pop();
    if (this.items.length > 0) {
      this.items[0] = last;
      this.bubbleDown(0);
    }
    return root;
  }

  bubbleUp(index) {
    while (index > 0) {
      const parent = Math.floor((index - 1) / 2);
      if (!this.isLowerPriority(this.items[index], this.items[parent])) break;
      [this.items[index], this.items[parent]] = [this.items[parent], this.items[index]];
      index = parent;
    }
  }

  bubbleDown(index) {
    const length = this.items.length;

    while (true) {
      const left = index * 2 + 1;
      const right = index * 2 + 2;
      let smallest = index;

      if (left < length && this.isLowerPriority(this.items[left], this.items[smallest])) {
        smallest = left;
      }
      if (right < length && this.isLowerPriority(this.items[right], this.items[smallest])) {
        smallest = right;
      }
      if (smallest === index) break;

      [this.items[index], this.items[smallest]] = [this.items[smallest], this.items[index]];
      index = smallest;
    }
  }
}

function findKthBusiestWithMinHeap(items, k) {
  const heap = new MinHeap((a, b) => {
    if (a.count !== b.count) return a.count < b.count;
    return a.date > b.date;
  });

  for (const item of items) {
    if (heap.size() < k) {
      heap.push(item);
    } else if (ranksHigher(item, heap.peek())) {
      heap.pop();
      heap.push(item);
    }
  }

  return heap.size() < k ? null : heap.peek();
}

function validatePositiveInteger(value, name, max) {
  if (!/^[1-9]\d*$/.test(value || "")) {
    return `${name} must be a positive integer`;
  }
  if (max && Number(value) > max) {
    return `${name} must be less than or equal to ${max}`;
  }
  return null;
}

function parseProductIds(value) {
  if (!value) {
    const error = new Error("productIds is required");
    error.status = 400;
    throw error;
  }

  const parts = value.split(",");
  const productIds = [];
  const seen = new Set();

  for (const part of parts) {
    const text = part.trim();
    if (!/^[1-9]\d*$/.test(text)) {
      const error = new Error("productIds must contain only positive integers");
      error.status = 400;
      throw error;
    }

    const productId = Number(text);
    if (!Number.isSafeInteger(productId)) {
      const error = new Error("productIds must contain safe positive integers");
      error.status = 400;
      throw error;
    }
    if (!seen.has(productId)) {
      seen.add(productId);
      productIds.push(productId);
    }
  }

  if (productIds.length === 0) {
    const error = new Error("productIds must include at least one product id");
    error.status = 400;
    throw error;
  }
  if (productIds.length > 10) {
    const error = new Error("productIds must contain at most 10 unique product ids");
    error.status = 400;
    throw error;
  }

  return productIds;
}

function validateLimit(value) {
  if (!value) {
    const error = new Error("limit is required");
    error.status = 400;
    throw error;
  }

  const message = validatePositiveInteger(value, "limit", 100);
  if (message) {
    const error = new Error(message);
    error.status = 400;
    throw error;
  }

  return Number(value);
}

function chunkArray(array, size) {
  const chunks = [];
  for (let index = 0; index < array.length; index += size) {
    chunks.push(array.slice(index, index + size));
  }
  return chunks;
}

async function mapWithConcurrency(items, limit, mapper) {
  const results = new Array(items.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < items.length) {
      const currentIndex = nextIndex;
      nextIndex += 1;
      results[currentIndex] = await mapper(items[currentIndex], currentIndex);
    }
  }

  const workerCount = Math.min(limit, items.length);
  await Promise.all(Array.from({ length: workerCount }, worker));
  return results;
}

async function fetchAllRentalsForRenter(userId) {
  if (!CENTRAL_API_TOKEN || CENTRAL_API_TOKEN === "your_team_token_here") {
    const error = new Error("Central API token is not configured");
    error.status = 500;
    throw error;
  }

  const rentals = [];
  let page = 1;
  let totalPages = 1;

  do {
    const path = `/api/data/rentals?renter_id=${encodeURIComponent(userId)}&page=${page}&limit=100`;
    const response = await fetch(`${CENTRAL_API_URL}${path}`, {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${CENTRAL_API_TOKEN}`
      },
      signal: AbortSignal.timeout(5000)
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
    totalPages = Number(parsed.totalPages || page);
    page += 1;
  } while (page <= totalPages);

  return rentals;
}

async function fetchProductsBatch(productIds) {
  if (productIds.length === 0) return [];

  const path = `/api/data/products/batch?ids=${productIds.map(encodeURIComponent).join(",")}`;
  const response = await fetch(`${CENTRAL_API_URL}${path}`, {
    headers: {
      accept: "application/json",
      authorization: `Bearer ${CENTRAL_API_TOKEN}`
    },
    signal: AbortSignal.timeout(5000)
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
}

function buildProductCategoryMap(products) {
  const productCategoryMap = new Map();
  for (const product of products) {
    if (product && product.id !== undefined && product.category) {
      productCategoryMap.set(Number(product.id), product.category);
    }
  }
  return productCategoryMap;
}

function countCategoriesFromRentals(rentals, productCategoryMap) {
  const categoryCounts = new Map();
  for (const rental of rentals) {
    const productId = Number(rental.productId);
    const category = productCategoryMap.get(productId);
    if (!category) continue;
    categoryCounts.set(category, (categoryCounts.get(category) || 0) + 1);
  }
  return categoryCounts;
}

function categoryRanksHigher(a, b) {
  if (a.rentalCount !== b.rentalCount) return a.rentalCount > b.rentalCount;
  return a.category < b.category;
}

function findTopKCategories(categoryCounts, k) {
  const heap = new MinHeap((a, b) => {
    if (a.rentalCount !== b.rentalCount) return a.rentalCount < b.rentalCount;
    return a.category > b.category;
  });

  for (const [category, rentalCount] of categoryCounts) {
    const item = { category, rentalCount };
    if (heap.size() < k) {
      heap.push(item);
    } else if (categoryRanksHigher(item, heap.peek())) {
      heap.pop();
      heap.push(item);
    }
  }

  const topCategories = [];
  while (heap.size() > 0) topCategories.push(heap.pop());
  topCategories.sort((a, b) => b.rentalCount - a.rentalCount || a.category.localeCompare(b.category));
  return topCategories;
}

async function handleTopCategoriesRequest(req, res, userIdText) {
  const userIdError = validatePositiveInteger(userIdText, "user id");
  if (userIdError) {
    return sendJson(res, 400, {
      error: "Top categories calculation failed",
      message: userIdError
    });
  }

  const sourceUrl = new URL(req.url, `http://${req.headers.host}`);
  const kText = sourceUrl.searchParams.get("k") || "3";
  const kError = validatePositiveInteger(kText, "k");
  if (kError) {
    return sendJson(res, 400, {
      error: "Top categories calculation failed",
      message: kError
    });
  }

  try {
    const userId = Number(userIdText);
    const k = Number(kText);
    const rentals = await fetchAllRentalsForRenter(userId);

    if (rentals.length === 0) {
      return sendJson(res, 200, { userId, topCategories: [] });
    }

    const uniqueProductIds = Array.from(
      new Set(rentals.map((rental) => Number(rental.productId)).filter(Number.isFinite))
    );
    const chunks = chunkArray(uniqueProductIds, 50);
    const productGroups = chunks.length <= 10
      ? await Promise.all(chunks.map(fetchProductsBatch))
      : await mapWithConcurrency(chunks, 5, fetchProductsBatch);
    const productCategoryMap = buildProductCategoryMap(productGroups.flat());
    const categoryCounts = countCategoriesFromRentals(rentals, productCategoryMap);

    return sendJson(res, 200, {
      userId,
      topCategories: findTopKCategories(categoryCounts, k)
    });
  } catch (error) {
    const status = error.status || 502;
    return sendJson(res, status, {
      error: "Top categories calculation failed",
      message: topCategoriesErrorMessage(status, error),
      details: error.upstream
    });
  }
}

function topCategoriesErrorMessage(status, error) {
  if (status === 500) return "Central API token is not configured";
  if (status === 404) return "Central API resource not found";
  if (status === 429) return "Central API rate limit exceeded";
  if (status >= 500 && status < 600) return "Central API error";
  return error.message || "Failed to calculate top categories";
}

async function fetchRentalStatsForMonth(month) {
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

    return parsed;
  } catch (error) {
    if (error.status) throw error;
    const upstreamError = new Error("Central API unavailable");
    upstreamError.status = 502;
    upstreamError.upstream = { message: error.message };
    throw upstreamError;
  }
}

async function handleKthBusiestDateRequest(req, res) {
  const sourceUrl = new URL(req.url, `http://${req.headers.host}`);
  const from = sourceUrl.searchParams.get("from");
  const to = sourceUrl.searchParams.get("to");
  const kText = sourceUrl.searchParams.get("k");

  if (!from) {
    return sendJson(res, 400, {
      error: "K-th busiest day calculation failed",
      message: "from is required"
    });
  }
  if (!to) {
    return sendJson(res, 400, {
      error: "K-th busiest day calculation failed",
      message: "to is required"
    });
  }
  if (!kText) {
    return sendJson(res, 400, {
      error: "K-th busiest day calculation failed",
      message: "k is required"
    });
  }
  if (!isValidMonth(from) || !isValidMonth(to)) {
    return sendJson(res, 400, {
      error: "K-th busiest day calculation failed",
      message: "from and to must be valid YYYY-MM strings"
    });
  }
  if (!/^[1-9]\d*$/.test(kText)) {
    return sendJson(res, 400, {
      error: "K-th busiest day calculation failed",
      message: "k must be a positive integer"
    });
  }

  const fromIndex = monthIndex(from);
  const toIndex = monthIndex(to);
  if (fromIndex > toIndex) {
    return sendJson(res, 400, {
      error: "K-th busiest day calculation failed",
      message: "from must be less than or equal to to"
    });
  }

  const months = generateMonthsBetween(from, to);
  if (months.length > 12) {
    return sendJson(res, 400, {
      error: "K-th busiest day calculation failed",
      message: "Date range must not exceed 12 months"
    });
  }

  try {
    const k = Number(kText);
    const statsResponses = await Promise.all(months.map(fetchRentalStatsForMonth));
    const dailyCounts = aggregateRentalStats(statsResponses);
    const kthBusiestDay = findKthBusiestWithMinHeap(dailyCounts, k);

    if (!kthBusiestDay) {
      return sendJson(res, 404, {
        error: "K-th busiest day calculation failed",
        message: "Not enough distinct rental dates to find the requested K-th busiest day"
      });
    }

    return sendJson(res, 200, {
      from,
      to,
      k,
      date: kthBusiestDay.date,
      rentalCount: kthBusiestDay.count
    });
  } catch (error) {
    const status = error.status || 502;
    return sendJson(res, status, {
      error: "K-th busiest day calculation failed",
      message: kthBusiestDateErrorMessage(status, error),
      details: error.upstream
    });
  }
}

function kthBusiestDateErrorMessage(status, error) {
  if (status === 500) return "Central API token is not configured";
  if (status === 429) return "Central API rate limit exceeded";
  if (status === 502) return error.message || "Central API unavailable";
  if (status >= 500 && status < 600) return "Central API error";
  return error.message || "Failed to calculate K-th busiest day";
}

function mergeIntervals(intervals) {
  const sorted = intervals
    .filter((interval) => interval.start <= interval.end)
    .sort((a, b) => a.start - b.start || a.end - b.end);
  const merged = [];

  for (const interval of sorted) {
    const last = merged[merged.length - 1];
    if (!last || interval.start > last.end + 1) {
      merged.push({ ...interval });
    } else {
      last.end = Math.max(last.end, interval.end);
    }
  }

  return merged;
}

function getFreeWindows(requestedFrom, requestedTo, busyPeriods) {
  const freeWindows = [];
  let cursor = requestedFrom;

  for (const period of busyPeriods) {
    if (period.end < requestedFrom) continue;
    if (period.start > requestedTo) break;

    const busyStart = Math.max(period.start, requestedFrom);
    const busyEnd = Math.min(period.end, requestedTo);

    if (cursor < busyStart) {
      freeWindows.push({ start: cursor, end: busyStart - 1 });
    }
    cursor = Math.max(cursor, busyEnd + 1);
  }

  if (cursor <= requestedTo) {
    freeWindows.push({ start: cursor, end: requestedTo });
  }

  return freeWindows;
}

function toDateRange(interval) {
  return {
    start: formatDateOnly(interval.start),
    end: formatDateOnly(interval.end)
  };
}

function isValidYear(value) {
  return /^(19|20)\d{2}$/.test(value || "");
}

function rentalToInterval(rental) {
  return {
    start: parseDateOnly(String(rental.rentalStart || "").slice(0, 10)),
    end: parseDateOnly(String(rental.rentalEnd || "").slice(0, 10))
  };
}

function clipIntervalToRange(interval, rangeStart, rangeEnd) {
  if (interval.start === null || interval.end === null) return null;

  const start = Math.max(interval.start, rangeStart);
  const end = Math.min(interval.end, rangeEnd);
  return start > end ? null : { start, end };
}

function findLongestFreeGap(rangeStart, rangeEnd, busyPeriods) {
  let cursor = rangeStart;
  let best = null;
  let maxDays = 0;

  function considerGap(start, end) {
    if (start > end) return;
    const days = end - start + 1;
    if (days > maxDays) {
      best = { start, end, days };
      maxDays = days;
    }
  }

  for (const period of busyPeriods) {
    if (period.start > cursor) {
      considerGap(cursor, period.start - 1);
    }
    cursor = Math.max(cursor, period.end + 1);
  }

  if (cursor <= rangeEnd) {
    considerGap(cursor, rangeEnd);
  }

  return best;
}

async function fetchAllRentalsForProduct(productId) {
  if (!CENTRAL_API_TOKEN || CENTRAL_API_TOKEN === "your_team_token_here") {
    const error = new Error("Central API token is not configured");
    error.status = 500;
    throw error;
  }

  const rentals = [];
  let page = 1;
  let totalPages = 1;

  do {
    const path = `/api/data/rentals?product_id=${encodeURIComponent(productId)}&page=${page}&limit=100`;
    const response = await fetch(`${CENTRAL_API_URL}${path}`, {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${CENTRAL_API_TOKEN}`
      },
      signal: AbortSignal.timeout(5000)
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
    totalPages = Number(parsed.totalPages || page);
    page += 1;
  } while (page <= totalPages);

  return rentals;
}

async function fetchRentalPageForProduct(productId, page, limit) {
  if (!CENTRAL_API_TOKEN || CENTRAL_API_TOKEN === "your_team_token_here") {
    const error = new Error("Central API token is not configured");
    error.status = 500;
    throw error;
  }

  const path = `/api/data/rentals?product_id=${encodeURIComponent(productId)}&page=${page}&limit=${limit}`;

  try {
    const response = await fetch(`${CENTRAL_API_URL}${path}`, {
      headers: {
        accept: "application/json",
        authorization: `Bearer ${CENTRAL_API_TOKEN}`
      },
      signal: AbortSignal.timeout(5000)
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

    const responseLimit = Number(parsed.limit || limit);
    const totalPages = Number(
      parsed.totalPages || (parsed.total && responseLimit > 0 ? Math.ceil(Number(parsed.total) / responseLimit) : page)
    );

    return {
      data: parsed.data,
      page: Number(parsed.page || page),
      totalPages: Number.isFinite(totalPages) && totalPages > 0 ? totalPages : page
    };
  } catch (error) {
    if (error.status) throw error;
    const upstreamError = new Error("Central API unavailable");
    upstreamError.status = 502;
    upstreamError.upstream = { message: error.message };
    throw upstreamError;
  }
}

async function initializeRentalStreams(productIds) {
  const firstPages = await Promise.all(
    productIds.map((productId) => fetchRentalPageForProduct(productId, 1, 100))
  );

  return firstPages.map((page, index) => ({
    productId: productIds[index],
    page: page.page,
    totalPages: page.totalPages,
    items: page.data
  }));
}

function rentalDateKey(value) {
  return String(value || "").slice(0, 10);
}

function normalizeRental(rental) {
  return {
    rentalId: Number(rental.id),
    productId: Number(rental.productId),
    rentalStart: rentalDateKey(rental.rentalStart),
    rentalEnd: rentalDateKey(rental.rentalEnd)
  };
}

function compareRentals(a, b) {
  const startCompare = rentalDateKey(a.rentalStart).localeCompare(rentalDateKey(b.rentalStart));
  if (startCompare !== 0) return startCompare;

  const productCompare = Number(a.productId) - Number(b.productId);
  if (productCompare !== 0) return productCompare;

  return Number(a.id) - Number(b.id);
}

function compareFeedNodes(a, b) {
  return compareRentals(a.rental, b.rental);
}

async function pushNextLoadedOrFetchedItem(heap, streams, streamIndex, itemIndex) {
  const stream = streams[streamIndex];

  if (itemIndex < stream.items.length) {
    heap.push({ rental: stream.items[itemIndex], streamIndex, itemIndex });
    return;
  }

  while (stream.page < stream.totalPages) {
    const nextPage = await fetchRentalPageForProduct(stream.productId, stream.page + 1, 100);
    const firstNewItemIndex = stream.items.length;
    stream.page = nextPage.page;
    stream.totalPages = nextPage.totalPages;
    stream.items.push(...nextPage.data);

    if (firstNewItemIndex < stream.items.length) {
      heap.push({
        rental: stream.items[firstNewItemIndex],
        streamIndex,
        itemIndex: firstNewItemIndex
      });
      return;
    }
  }
}

async function mergeStreamsWithMinHeap(streams, limit) {
  const heap = new MinHeap((a, b) => compareFeedNodes(a, b) < 0);
  const feed = [];

  for (let streamIndex = 0; streamIndex < streams.length; streamIndex += 1) {
    const stream = streams[streamIndex];
    if (stream.items.length > 0) {
      heap.push({ rental: stream.items[0], streamIndex, itemIndex: 0 });
    }
  }

  while (heap.size() > 0 && feed.length < limit) {
    const node = heap.pop();
    feed.push(normalizeRental(node.rental));

    if (feed.length >= limit) break;

    await pushNextLoadedOrFetchedItem(heap, streams, node.streamIndex, node.itemIndex + 1);
  }

  return feed;
}

function mergedFeedErrorMessage(status, error) {
  if (status === 500) return "Central API token is not configured";
  if (status === 404) return "Central API rental stream not found";
  if (status === 429) return "Central API rate limit exceeded";
  if (status === 502) return error.message || "Central API unavailable";
  if (status >= 500 && status < 600) return "Central API error";
  return error.message || "Failed to calculate merged rental feed";
}

async function handleMergedFeedRequest(req, res) {
  const sourceUrl = new URL(req.url, `http://${req.headers.host}`);
  let productIds;
  let limit;

  try {
    productIds = parseProductIds(sourceUrl.searchParams.get("productIds"));
    limit = validateLimit(sourceUrl.searchParams.get("limit"));
  } catch (error) {
    return sendJson(res, error.status || 400, {
      error: "Merged feed calculation failed",
      message: error.message
    });
  }

  try {
    const streams = await initializeRentalStreams(productIds);
    const feed = await mergeStreamsWithMinHeap(streams, limit);

    return sendJson(res, 200, {
      productIds,
      limit,
      feed
    });
  } catch (error) {
    const status = error.status || 502;
    return sendJson(res, status, {
      error: "Merged feed calculation failed",
      message: mergedFeedErrorMessage(status, error),
      details: error.upstream
    });
  }
}

async function handleAvailabilityRequest(req, res, productIdText) {
  if (!/^[1-9]\d*$/.test(productIdText)) {
    return sendJson(res, 400, {
      error: "Availability check failed",
      message: "Invalid product id"
    });
  }

  const sourceUrl = new URL(req.url, `http://${req.headers.host}`);
  const from = sourceUrl.searchParams.get("from");
  const to = sourceUrl.searchParams.get("to");

  if (!from) {
    return sendJson(res, 400, { error: "Availability check failed", message: "from is required" });
  }
  if (!to) {
    return sendJson(res, 400, { error: "Availability check failed", message: "to is required" });
  }

  const requestedFrom = parseDateOnly(from);
  const requestedTo = parseDateOnly(to);
  if (requestedFrom === null || requestedTo === null) {
    return sendJson(res, 400, {
      error: "Availability check failed",
      message: "from and to must be valid YYYY-MM-DD dates"
    });
  }
  if (requestedFrom > requestedTo) {
    return sendJson(res, 400, {
      error: "Availability check failed",
      message: "from must be less than or equal to to"
    });
  }

  try {
    const productId = Number(productIdText);
    const rentals = await fetchAllRentalsForProduct(productId);
    const intervals = rentals
      .map((rental) => ({
        start: parseDateOnly(String(rental.rentalStart || "").slice(0, 10)),
        end: parseDateOnly(String(rental.rentalEnd || "").slice(0, 10))
      }))
      .filter((interval) => interval.start !== null && interval.end !== null);
    const merged = mergeIntervals(intervals);
    const overlappingBusy = merged.filter(
      (interval) => interval.start <= requestedTo && interval.end >= requestedFrom
    );
    const freeWindows = getFreeWindows(requestedFrom, requestedTo, merged);

    return sendJson(res, 200, {
      productId,
      from,
      to,
      available: overlappingBusy.length === 0,
      busyPeriods: overlappingBusy.map(toDateRange),
      freeWindows: freeWindows.map(toDateRange)
    });
  } catch (error) {
    const status = error.status || 502;
    return sendJson(res, status, {
      error: "Availability check failed",
      message: availabilityErrorMessage(status, error),
      details: error.upstream
    });
  }
}

function availabilityErrorMessage(status, error) {
  if (status === 500) return "Central API token is not configured";
  if (status === 404) return "Central API resource not found";
  if (status === 429) return "Central API rate limit exceeded";
  if (status >= 500 && status < 600) return "Central API error";
  return error.message || "Failed to fetch rentals from Central API";
}

async function handleFreeStreakRequest(req, res, productIdText) {
  if (!/^[1-9]\d*$/.test(productIdText)) {
    return sendJson(res, 400, {
      error: "Free streak calculation failed",
      message: "Invalid product id"
    });
  }

  const sourceUrl = new URL(req.url, `http://${req.headers.host}`);
  const yearText = sourceUrl.searchParams.get("year");
  if (!yearText) {
    return sendJson(res, 400, {
      error: "Free streak calculation failed",
      message: "year is required"
    });
  }
  if (!isValidYear(yearText)) {
    return sendJson(res, 400, {
      error: "Free streak calculation failed",
      message: "year must be a valid YYYY value"
    });
  }

  try {
    const productId = Number(productIdText);
    const year = Number(yearText);
    const yearStart = parseDateOnly(`${yearText}-01-01`);
    const yearEnd = parseDateOnly(`${yearText}-12-31`);
    const rentals = await fetchAllRentalsForProduct(productId);
    const clippedIntervals = rentals
      .map(rentalToInterval)
      .map((interval) => clipIntervalToRange(interval, yearStart, yearEnd))
      .filter(Boolean);
    const busyIntervals = mergeIntervals(clippedIntervals);
    const longest = findLongestFreeGap(yearStart, yearEnd, busyIntervals);

    return sendJson(res, 200, {
      productId,
      year,
      longestFreeStreak: longest
        ? {
            from: formatDateOnly(longest.start),
            to: formatDateOnly(longest.end),
            days: longest.days
          }
        : {
            from: null,
            to: null,
            days: 0
          }
    });
  } catch (error) {
    const status = error.status || 502;
    return sendJson(res, status, {
      error: "Free streak calculation failed",
      message: availabilityErrorMessage(status, error),
      details: error.upstream
    });
  }
}

async function getValidCategories() {
  const now = Date.now();
  if (categoryCache.categories && categoryCache.expiresAt > now) {
    return categoryCache.categories;
  }

  if (!categoryCache.pending) {
    categoryCache.pending = fetchCentralJson("/api/data/categories")
      .then((data) => {
        if (!data || !Array.isArray(data.categories)) {
          const error = new Error("Central API returned an invalid categories response");
          error.status = 502;
          throw error;
        }

        categoryCache.categories = data.categories;
        categoryCache.expiresAt = Date.now() + CATEGORY_CACHE_TTL_MS;
        return categoryCache.categories;
      })
      .finally(() => {
        categoryCache.pending = null;
      });
  }

  return categoryCache.pending;
}

async function validateProductListRequest(req, res) {
  const sourceUrl = new URL(req.url, `http://${req.headers.host}`);
  const category = sourceUrl.searchParams.get("category");
  if (!category) return true;

  try {
    const validCategories = await getValidCategories();
    const upperCategory = category.toUpperCase();
    if (validCategories.includes(upperCategory)) {
      sourceUrl.searchParams.set("category", upperCategory);
      req.url = `${sourceUrl.pathname}?${sourceUrl.searchParams.toString()}`;
      return true;
    }

    sendJson(res, 400, {
      error: `Invalid category "${category}"`,
      message: "Use one of the valid category strings exactly as listed.",
      validCategories
    });
    return false;
  } catch (error) {
    sendJson(res, error.status || 502, {
      error: "Could not validate category",
      detail: error.message,
      upstream: error.upstream
    });
    return false;
  }
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
  if (status === 404) return "Product not found";
  if (status === 429) return "Central API rate limit exceeded";
  if (status >= 500) return "Central API error";
  if (status === 401 || status === 403) return "Central API authentication failed";
  return "Central API rejected the request";
}

http
  .createServer(async (req, res) => {
    const path = req.url.split("?")[0];
    if (req.method === "GET" && path === "/status") {
      return sendJson(res, 200, { service: "rental-service", status: "OK" });
    }
    if (req.method === "GET" && path === "/rentals/products") {
      const isValid = await validateProductListRequest(req, res);
      if (!isValid) return;
      return proxyCentral(req, res, "/api/data/products");
    }
    if (req.method === "GET" && path === "/rentals/kth-busiest-date") {
      return handleKthBusiestDateRequest(req, res);
    }
    if (req.method === "GET" && path === "/rentals/merged-feed") {
      return handleMergedFeedRequest(req, res);
    }
    const topCategoriesMatch = path.match(/^\/rentals\/users\/([^/]+)\/top-categories$/);
    if (req.method === "GET" && topCategoriesMatch) {
      return handleTopCategoriesRequest(req, res, topCategoriesMatch[1]);
    }
    const freeStreakMatch = path.match(/^\/rentals\/products\/([^/]+)\/(?:free-streak|longest-free-streak)$/);
    if (req.method === "GET" && freeStreakMatch) {
      return handleFreeStreakRequest(req, res, freeStreakMatch[1]);
    }
    const availabilityMatch = path.match(/^\/rentals\/products\/([^/]+)\/availability$/);
    if (req.method === "GET" && availabilityMatch) {
      return handleAvailabilityRequest(req, res, availabilityMatch[1]);
    }
    if (req.method === "GET" && path.startsWith("/rentals/products/")) {
      const id = path.replace("/rentals/products/", "");
      if (!id) return sendJson(res, 400, { error: "Product id is required" });
      return proxyCentral(req, res, `/api/data/products/${encodeURIComponent(id)}`);
    }
    sendJson(res, 404, { error: "Route not found" });
  })
  .listen(PORT, () => console.log(`rental-service listening on ${PORT}`));
