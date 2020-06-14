FROM node:lts-alpine
LABEL version="1.0.0"
LABEL maintainer="shinjan@cse.iitb.ac.in"

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

ENV NODE_ENV=production \
	REMOTE_BROKER_URL=mqtt://localhost \
    GATEWAY_SERIAL=oneboard-gateway-000

CMD ["npm", "run", "start"]
