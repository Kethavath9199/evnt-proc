FROM node:16.16.0 as build

WORKDIR /home/node/app

COPY ./package.json ./

COPY . .

RUN npm i

RUN npm i nodemailer --save

RUN chmod -R 777 /home/node/app/

RUN npm run build

CMD ["node" ,  "dist/main.js" ]