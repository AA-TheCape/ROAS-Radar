FROM node:22.13.0-bookworm-slim AS deps
WORKDIR /app

COPY package.json package-lock.json ./
# The root postinstall installs dashboard dev dependencies, which are not needed
# for the API image and are not present in this Docker build layer.
RUN npm ci --ignore-scripts

FROM deps AS build
COPY tsconfig.json ./
COPY packages ./packages
COPY src ./src
RUN npm run build

FROM node:22.13.0-bookworm-slim AS runtime
WORKDIR /app
ENV NODE_ENV=production

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts

COPY --from=build /app/dist ./dist
COPY db ./db

EXPOSE 8080
CMD ["npm", "run", "start:api"]
