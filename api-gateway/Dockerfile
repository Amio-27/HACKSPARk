FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production
# copy package files first for better layer caching
COPY package*.json ./
# prefer npm ci when a lockfile exists, fall back to install if not
RUN if [ -f package-lock.json ]; then npm ci --omit=dev; else npm install --omit=dev; fi && npm cache clean --force
COPY server.js ./
USER node
EXPOSE 8000
CMD ["node", "server.js"]
