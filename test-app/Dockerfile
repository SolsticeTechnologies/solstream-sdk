FROM node:20-alpine

# Install SDK at /js so that test-app's "file:../js" resolves correctly
WORKDIR /js
COPY js/package*.json ./
RUN npm install
COPY js/ ./
RUN npm run build

# Install test-app
WORKDIR /app
COPY test-app/package*.json ./
RUN npm install
COPY test-app/ ./

# MONITOR is the script name to run, e.g. monitor-slots
ARG MONITOR=monitor-slots
ENV MONITOR=${MONITOR}

CMD ["sh", "-c", "npx ts-node src/${MONITOR}.ts"]
