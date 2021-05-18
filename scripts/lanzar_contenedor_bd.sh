#!/bin/bash

[[ $(docker ps -f "name=p2_mongo" --format '{{.Names}}') == p2_mongo ]] ||
docker run --name p2_mongo -dp 27017:27017 -e MONGO_INITDB_ROOT_USERNAME=root -e MONGO_INITDB_ROOT_PASSWORD=pass mongo:latest
