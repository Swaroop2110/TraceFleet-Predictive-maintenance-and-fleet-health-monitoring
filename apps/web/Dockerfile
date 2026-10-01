# Development image: Vite dev server with hot reload, which is what the Compose
# demo uses. `--target production` builds the static bundle behind nginx instead.
FROM node:20-alpine AS deps

WORKDIR /app
COPY package.json package-lock.json ./
# `npm ci` for a reproducible install from the lockfile. `npm install` can resolve
# differently from what was tested.
RUN npm ci --legacy-peer-deps


FROM node:20-alpine AS development

ENV NODE_ENV=development
WORKDIR /app
COPY --from=deps --chown=node:node /app/node_modules ./node_modules
COPY . .

# node:alpine ships a `node` user already.
USER node
EXPOSE 5173
CMD ["npm", "run", "dev"]


FROM node:20-alpine AS build

WORKDIR /app
COPY --from=deps --chown=node:node /app/node_modules ./node_modules
COPY . .
RUN npm run build


FROM nginx:1.27-alpine AS production
ENV API_UPSTREAM=api:8000
RUN mkdir -p /etc/nginx/templates

# SPA routing: unknown paths fall through to index.html so a deep link works on
# reload. The API/WebSocket proxy uses the deployment's API_UPSTREAM.
RUN printf '%s\n' \
    'server {' \
    '  listen 8080;' \
    '  root /usr/share/nginx/html;' \
    '  index index.html;' \
    '  add_header X-Content-Type-Options "nosniff" always;' \
    '  add_header X-Frame-Options "DENY" always;' \
    '  add_header Referrer-Policy "no-referrer" always;' \
    '  location /api/ { proxy_pass http://${API_UPSTREAM}; proxy_http_version 1.1; proxy_set_header Upgrade $http_upgrade; proxy_set_header Connection "upgrade"; }' \
    '  location / { try_files $uri $uri/ /index.html; }' \
    '  location /assets/ { expires 1y; add_header Cache-Control "public, immutable"; }' \
    '}' > /etc/nginx/templates/default.conf.template \
 && rm -f /etc/nginx/conf.d/default.conf.default 2>/dev/null || true

COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 8080
HEALTHCHECK --interval=15s --timeout=3s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/ >/dev/null || exit 1
