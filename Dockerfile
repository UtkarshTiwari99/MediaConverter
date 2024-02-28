FROM node:18.16.1

LABEL authors="UTKARSH TIWARI"

COPY . /home/mediaConverter

WORKDIR /home/mediaConverter

RUN npm install

RUN apt update && apt install ffmpeg -y

CMD ["node", "testIndex.js"]