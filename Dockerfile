FROM node:22-bookworm-slim AS deps
WORKDIR /app/dashboard

COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci --include=dev
RUN ln -s /app/dashboard/node_modules /app/node_modules

FROM deps AS build
WORKDIR /app
COPY packages ./packages
COPY dashboard ./dashboard
WORKDIR /app/dashboard
RUN npm run build

FROM node:22-bookworm-slim AS runtime
WORKDIR /app
ENV NODE_ENV=production

COPY --from=build /app/dashboard/dist ./dist
COPY --from=build /app/dashboard/server.mjs ./

EXPOSE 8080
CMD ["node", "server.mjs"]
