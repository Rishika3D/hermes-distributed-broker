# Hermes dashboard: Next.js standalone production build.
# Broker URLs and the API key are RUNTIME env (HERMES_BROKERS / HERMES_API_KEY), read by the
# server-side /api/poll route; nothing broker-specific is baked into the image or the browser.
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM node:22-alpine
# Patch OS packages, and drop npm/corepack/yarn: the server needs only `node`, and the package
# managers bundle their own dependency trees (the bulk of this image's earlier CVE findings).
RUN apk upgrade --no-cache \
    && rm -rf /usr/local/lib/node_modules/npm /usr/local/lib/node_modules/corepack \
              /usr/local/bin/npm /usr/local/bin/npx /usr/local/bin/corepack \
              /opt/yarn-* /usr/local/bin/yarn /usr/local/bin/yarnpkg
WORKDIR /app
ENV NODE_ENV=production HOSTNAME=0.0.0.0 PORT=3000
COPY --from=build --chown=node:node /app/.next/standalone ./
COPY --from=build --chown=node:node /app/.next/static ./.next/static
USER node
EXPOSE 3000
HEALTHCHECK --interval=15s --timeout=3s --start-period=15s \
  CMD wget -qO- http://127.0.0.1:3000/api/healthz > /dev/null || exit 1
# `node` is PID 1 and receives SIGTERM directly.
CMD ["node", "server.js"]
