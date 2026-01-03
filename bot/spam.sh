#!/bin/bash

go build random.go client.go

for i in {1..100}
do
    ./random &
    sleep 1
done

wait