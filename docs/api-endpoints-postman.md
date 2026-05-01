# RentPi API Endpoints for Postman

Base URL:

```text
http://localhost:8000
```

Postman-e test korar jonno API Gateway use korben. Full URL gulo niche deya ache, tai direct copy kore request URL field-e paste kora jabe.

## Health Check

| Method | URL | Kaj |
|---|---|---|
| GET | `http://localhost:8000/status` | API Gateway ebong downstream service status check kore. |

Copy line:

```text
GET http://localhost:8000/status - API Gateway ebong downstream service status check kore.
```

## User APIs

| Method | URL | Kaj |
|---|---|---|
| POST | `http://localhost:8000/users/register` | Notun user register kore, response-e JWT token ebong user info dey. |
| POST | `http://localhost:8000/users/login` | Existing user login kore, response-e JWT token ebong user info dey. |
| GET | `http://localhost:8000/users/me` | Bearer token diye logged-in user info fetch kore. |
| GET | `http://localhost:8000/users/1/discount` | Central API theke user security score niye discount percent calculate kore. |

Copy lines:

```text
POST http://localhost:8000/users/register - Notun user register kore, response-e JWT token ebong user info dey.
POST http://localhost:8000/users/login - Existing user login kore, response-e JWT token ebong user info dey.
GET http://localhost:8000/users/me - Bearer token diye logged-in user info fetch kore.
GET http://localhost:8000/users/1/discount - Central API theke user security score niye discount percent calculate kore.
```

Register body:

```json
{
  "name": "Test User",
  "email": "test@example.com",
  "password": "123456"
}
```

Login body:

```json
{
  "email": "test@example.com",
  "password": "123456"
}
```

Users me header:

```text
Authorization: Bearer {{token}}
```

## Rental APIs

| Method | URL | Kaj |
|---|---|---|
| GET | `http://localhost:8000/rentals/products?page=1&limit=10` | Central API theke product list fetch kore. |
| GET | `http://localhost:8000/rentals/products?category=ELECTRONICS&page=1&limit=10` | Category filter diye product list fetch kore. Category valid na hole valid category list return kore. |
| GET | `http://localhost:8000/rentals/products/1` | Specific product details fetch kore. |
| GET | `http://localhost:8000/rentals/products/1/availability?from=2026-05-01&to=2026-05-10` | Specific date range-e product available kina check kore, busy periods ebong free windows dey. |
| GET | `http://localhost:8000/rentals/products/1/free-streak?year=2026` | Specific year-e product-er longest free streak calculate kore. |
| GET | `http://localhost:8000/rentals/products/1/longest-free-streak?year=2026` | `free-streak` endpoint-er same kaj kore, alternative route. |
| GET | `http://localhost:8000/rentals/kth-busiest-date?from=2026-01&to=2026-03&k=3` | Month range-er moddhe k-th busiest rental date ber kore. |
| GET | `http://localhost:8000/rentals/merged-feed?productIds=1,2,3&limit=20` | Multiple product-er rental stream merge kore sorted feed dey. |
| GET | `http://localhost:8000/rentals/users/1/top-categories?k=3` | User/renter-er rental history theke top k categories calculate kore. |

Copy lines:

```text
GET http://localhost:8000/rentals/products?page=1&limit=10 - Central API theke product list fetch kore.
GET http://localhost:8000/rentals/products?category=ELECTRONICS&page=1&limit=10 - Category filter diye product list fetch kore.
GET http://localhost:8000/rentals/products/1 - Specific product details fetch kore.
GET http://localhost:8000/rentals/products/1/availability?from=2026-05-01&to=2026-05-10 - Product available kina check kore.
GET http://localhost:8000/rentals/products/1/free-streak?year=2026 - Product-er longest free streak calculate kore.
GET http://localhost:8000/rentals/products/1/longest-free-streak?year=2026 - Product-er longest free streak calculate kore, alternative route.
GET http://localhost:8000/rentals/kth-busiest-date?from=2026-01&to=2026-03&k=3 - K-th busiest rental date ber kore.
GET http://localhost:8000/rentals/merged-feed?productIds=1,2,3&limit=20 - Multiple product rental stream merge kore.
GET http://localhost:8000/rentals/users/1/top-categories?k=3 - User-er top rental categories ber kore.
```

## Analytics APIs

| Method | URL | Kaj |
|---|---|---|
| GET | `http://localhost:8000/analytics/summary` | Analytics service ready kina simple summary response dey. |
| GET | `http://localhost:8000/analytics/peak-window?from=2026-01&to=2026-03` | Month range-er moddhe highest 7-day rental window calculate kore. |
| GET | `http://localhost:8000/analytics/surge-days?month=2026-05` | Ekta month-er prottek diner next higher rental surge day calculate kore. |
| GET | `http://localhost:8000/analytics/recommendations?date=2026-05-15&limit=5` | Historical seasonal demand use kore product recommendation dey. |

Copy lines:

```text
GET http://localhost:8000/analytics/summary - Analytics service ready kina simple summary response dey.
GET http://localhost:8000/analytics/peak-window?from=2026-01&to=2026-03 - Highest 7-day rental window calculate kore.
GET http://localhost:8000/analytics/surge-days?month=2026-05 - Month-er next surge days calculate kore.
GET http://localhost:8000/analytics/recommendations?date=2026-05-15&limit=5 - Seasonal product recommendation dey.
```

## Chat / Agent APIs

| Method | URL | Kaj |
|---|---|---|
| GET | `http://localhost:8000/chat/sessions` | Existing in-memory chat sessions list kore. |
| GET | `http://localhost:8000/chat/demo/history` | `demo` session-er chat history fetch kore. `demo` er jaygay onno sessionId deya jabe. |
| POST | `http://localhost:8000/chat/message` | Chat session-e message pathay ebong assistant reply return kore. |

Copy lines:

```text
GET http://localhost:8000/chat/sessions - Existing in-memory chat sessions list kore.
GET http://localhost:8000/chat/demo/history - demo session-er chat history fetch kore.
POST http://localhost:8000/chat/message - Chat session-e message pathay ebong assistant reply return kore.
```

Chat message body:

```json
{
  "sessionId": "demo",
  "message": "Hello"
}
```

## Direct Service URLs

Normally Postman-e gateway URL `http://localhost:8000` use korlei enough. Tobe direct service test korte chaile:

| Service | URL | Kaj |
|---|---|---|
| Frontend | `http://localhost:3000` | Browser frontend. |
| User Service | `http://localhost:8001/status` | Direct user-service status. |
| Rental Service | `http://localhost:8002/status` | Direct rental-service status. |
| Analytics Service | `http://localhost:8003/status` | Direct analytics-service status. |
| Agentic Service | `http://localhost:8004/status` | Direct agentic-service status. |

Copy lines:

```text
GET http://localhost:8001/status - Direct user-service status.
GET http://localhost:8002/status - Direct rental-service status.
GET http://localhost:8003/status - Direct analytics-service status.
GET http://localhost:8004/status - Direct agentic-service status.
```

## Important Notes

- Docker Compose run thakle gateway URL `http://localhost:8000` kaj korbe.
- Central API token server-side `.env` file theke use hoy. Token Postman-e ba public jaygay paste/share korben na.
- `{{token}}` value paben `/users/register` ba `/users/login` response theke.
