# Build stage
FROM node:18-alpine AS builder

WORKDIR /app

# Copier les dépendances
COPY package.json package-lock.json ./

# Installer les dépendances
RUN npm install

# Copier le code
COPY . .

# Build
RUN npm run build


# Production stage
FROM nginx:alpine

# Copier le build
COPY --from=builder /app/dist /usr/share/nginx/html

# Config nginx
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --quiet --tries=1 --spider http://localhost/ || exit 1

CMD ["nginx", "-g", "daemon off;"]