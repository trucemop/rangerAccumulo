#!/bin/bash

git clone https://github.com/trucemop/docker-kerberos.git
git clone https://github.com/trucemop/docker-hdp.git
git clone https://github.com/trucemop/docker-nifi.git

cp docker-hdp/.env .

mvn clean package -f ./plugin

rm -f ./docker-hdp/containers/node/ranger-accumulo-plugin-*.jar
mv ./plugin/target/ranger-accumulo-plugin-*.jar ./docker-hdp/containers/node/

docker-compose -f docker-hdp/examples/compose/accumulo-single-container.yml build
