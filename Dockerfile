FROM node:22.23.4-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY . .
ENV PORT=8080
EXPOSE 8080
CMD ["node", "server.js"]
