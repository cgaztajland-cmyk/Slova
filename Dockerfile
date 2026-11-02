FROM node:20-slim
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY . .
RUN node get-words.js
ENV PORT=7860
ENV TRUST_PROXY=1
EXPOSE 7860
USER node
CMD ["node", "server.js"]
