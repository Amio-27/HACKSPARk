FROM node:20-alpine AS builder
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY index.html ./
COPY status ./

FROM busybox:1.36.1 AS runtime
WORKDIR /www
COPY --from=builder /app/index.html ./
COPY --from=builder /app/status ./status
EXPOSE 3000
CMD ["httpd", "-f", "-p", "3000", "-h", "/www"]
