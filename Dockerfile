FROM node:22-alpine
ENV NODE_ENV=production
WORKDIR /app
COPY package.json ./
COPY src ./src
RUN mkdir -p /data && chown -R node:node /app /data
USER node
ENV DATA_DIR=/data HOST=0.0.0.0 PORT=3001
EXPOSE 3001
VOLUME ["/data"]
CMD ["node", "src/server.js"]
