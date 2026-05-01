FROM node:20-alpine AS builder
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY server.js ./

FROM alpine:3.20 AS runtime
RUN apk add --no-cache nodejs
WORKDIR /app
ENV NODE_ENV=production
COPY --from=builder /app ./
EXPOSE 8004
CMD ["node", "server.js"]
