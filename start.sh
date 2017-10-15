#!/bin/bash

docker-compose -f ./docker-hdp/examples/compose/accumulo-single-container.yml up -d


until $(curl --output /dev/null --silent --head --fail -u admin:admin http://localhost:8080/api/v1/clusters); do
	echo Waiting for Ambari to start...
	sleep 5
done

echo Submitting blueprint
./docker-hdp/submit-blueprint.sh single-container ./docker-hdp/examples/blueprints/single-container-accumulo.json


STATUS=""
while [ "${STATUS}" != "STARTED" ];do 
	echo Waiting to add policies...
	sleep 5
	STATUS=`curl -k -u admin:admin -H "X-Requested-By:ambari" -s -X GET "http://localhost:8080/api/v1/clusters/dev/hosts/dn0.dev/host_components/NAMENODE" | grep "\"state\"" | cut -d'"' -f4`
done

cd docker-hdp/containers/node/scripts

echo Init ranger accumulo policies
./addServiceType.sh
./addService.sh
./cleanPolicies.sh
./addPolicies.sh

