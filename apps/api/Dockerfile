# --- build stage ---
FROM node:20-alpine AS build
WORKDIR /app
RUN corepack enable && corepack prepare yarn@4.6.0 --activate

# Copy workspace root files
COPY package.json yarn.lock .yarnrc.yml ./
COPY apps/api/package.json apps/api/package.json

RUN yarn install

COPY apps/api apps/api
WORKDIR /app/apps/api
RUN yarn build

# --- runtime stage ---
FROM node:20-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production

# Copy only compiled output + node_modules needed for api workspace
COPY --from=build /app/node_modules /app/node_modules
COPY --from=build /app/apps/api/dist /app/apps/api/dist
COPY --from=build /app/apps/api/package.json /app/apps/api/package.json

WORKDIR /app/apps/api
EXPOSE 3000
CMD ["yarn", "start"]
