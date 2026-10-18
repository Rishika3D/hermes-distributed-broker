# Hermes dashboard: Next.js production build.
# Broker URLs and the API key are RUNTIME env (HERMES_BROKERS / HERMES_API_KEY), read by the
# server-side /api/poll route; nothing broker-specific is baked into the image or the browser.
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY --from=build --chown=node:node /app/package.json /app/package-lock.json ./
COPY --from=build --chown=node:node /app/node_modules ./node_modules
COPY --from=build --chown=node:node /app/.next ./.next
COPY --from=build --chown=node:node /app/next.config.mjs ./
USER node
EXPOSE 3000
HEALTHCHECK --interval=15s --timeout=3s --start-period=15s \
  CMD wget -qO- http://localhost:3000/api/healthz > /dev/null || exit 1
# Run next directly (not via npm) so it is PID 1 and receives SIGTERM.
CMD ["node_modules/.bin/next", "start", "-p", "3000"]
