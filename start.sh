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
	STATUS=`curl -k -u admin:admin -H "X-Requested-By:ambari" -s -X GET "http://localhost:8080/api/v1/clusters/dev/hosts/dn0.dev/host_components/RANGER_ADMIN" | grep "\"state\"" | cut -d'"' -f4`
done

echo Adding users...
docker exec -it compose_dn0.dev_1 /root/addUsers.sh

cd docker-hdp/containers/node/scripts

echo Init ranger accumulo policies...
./addServiceType.sh
./addService.sh
./cleanPolicies.sh
./addPolicies.sh

cd -


STATUS=""
while [ "${STATUS}" != "STARTED" ];do
        echo Waiting to insert test table...
        sleep 5
        STATUS=`curl -k -u admin:admin -H "X-Requested-By:ambari" -s -X GET "http://localhost:8080/api/v1/clusters/dev/hosts/dn0.dev/host_components/ACCUMULO_TRACER" | grep "\"state\"" | cut -d'"' -f4`
done

sleep 5

echo Creating sample table...
docker cp statements compose_dn0.dev_1:/root/
docker cp insert.sh compose_dn0.dev_1:/root/
docker exec -u root -it compose_dn0.dev_1 sh -c '/root/insert.sh'

