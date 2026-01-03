FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y build-essential make g++ nlohmann-json3-dev && \
    apt-get clean && rm -r /var/lib/apt/lists/*

WORKDIR /app

COPY include/ ./include/
COPY src/ ./src/
COPY Makefile .
COPY schema.json .

RUN make

EXPOSE 7432

CMD ["./interface", "-l"]