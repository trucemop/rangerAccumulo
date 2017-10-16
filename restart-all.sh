#!/bin/bash

# This script bounces the cluster. Auto-Start has to be turned off first, or it will interfere with the start all command.

ambariServerIp=localhost
clusterName="dev"

#Get Tag and Properties Block for Current Stack Settings
versionTag=`curl -su admin:admin -H "X-Requested-By: ambari" -X GET "http://$ambariServerIp:8080/api/v1/clusters/$clusterName?fields=Clusters/desired_configs/cluster-env" | grep "\"tag\"" | cut -d '"' -f 4`

propertiesBlock=`curl -su admin:admin -H "X-Requested-By: ambari" -X GET "http://$ambariServerIp:8080/api/v1/clusters/$clusterName/configurations?type=cluster-env&tag=$versionTag" | sed -n -e '/properties/,$p' | head -n -3 | sed 's/"recovery_enabled" : "true"/"recovery_enabled" : "false"/g'`

# Tell Ambari to stop being helpful.
echo '{"Clusters":{"desired_config":{"type":"cluster-env","tag":"TurnOffAutoStart",' > data.json
echo "$propertiesBlock" >> data.json
echo '}}}' >> data.json

curl -su admin:admin -H "X-Requested-By: ambari" -X PUT -d "@./data.json" "http://$ambariServerIp:8080/api/v1/clusters/$clusterName"

# Tell Ambari to stop everything.
curl -i -su admin:admin -H "X-Requested-By: ambari"  -X PUT  -d '{"RequestInfo":{"context":"_PARSE_.STOP.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"$clusterName"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'  http://$ambariServerIp:8080/api/v1/clusters/$clusterName/services?

sleep 10

jobHref=`curl -su admin:admin -H "X-Requested-By: ambari" -X GET "http://$ambariServerIp:8080/api/v1/clusters/$clusterName/requests" | grep "\"href\"" | tail -1 | cut -d '"' -f 4`

# Wait for Ambari to stop everything.
while true;do
        jobStatus=`curl -su admin:admin -H "X-Requested-By: ambari" -X GET "$jobHref" | grep "\"request_status\"" | cut -d '"' -f4`

        if [ "$jobStatus" == "COMPLETED" ];then break;fi

        if [ "$jobStatus" == "FAILED" ];then break;fi
        if [ "$jobStatus" == "ABORTED" ];then break;fi

        echo "Still running: ${jobStatus}"
        sleep 5
done

if [ "$jobStatus" == "FAILED" ];then echo "Ambari Stop All Failed" && exit 1;fi
if [ "$jobStatus" == "ABORTED" ];then echo "Ambari Stop All Aborted" && exit 1;fi

sleep 30

# Tell Ambari to start everything.
curl -i -su admin:admin -H "X-Requested-By: ambari"  -X PUT  -d '{"RequestInfo":{"context":"_PARSE_.START.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"$clusterName"}},"Body":{"ServiceInfo":{"state":"STARTED"}}}' http://$ambariServerIp:8080/api/v1/clusters/$clusterName/services?

sleep 10

jobHref=`curl -su admin:admin -H "X-Requested-By: ambari" -X GET "http://$ambariServerIp:8080/api/v1/clusters/$clusterName/requests" | grep "\"href\"" | tail -1 | cut -d '"' -f 4`

# Wait for Ambari to start everything.
while true;do
        jobStatus=`curl -su admin:admin -H "X-Requested-By: ambari" -X GET "$jobHref" | grep "\"request_status\"" | cut -d '"' -f4`

        if [ "$jobStatus" == "COMPLETED" ];then break;fi

        if [ "$jobStatus" == "FAILED" ];then break;fi
        if [ "$jobStatus" == "ABORTED" ];then break;fi

        echo "Still running: ${jobStatus}"
        sleep 5
done

if [ "$jobStatus" == "FAILED" ];then echo "Ambari Start All Failed" && exit 1;fi
if [ "$jobStatus" == "ABORTED" ];then echo "Ambari Start All Aborted" && exit 1;fi

# Tell Ambari to start being helpful again.
propertiesBlock=`echo "$propertiesBlock" | sed 's/"recovery_enabled" : "false"/"recovery_enabled" : "true"/g'`
echo '{"Clusters":{"desired_config":{"type":"cluster-env","tag":"TurnOnAutoStart",' > data.json
echo "$propertiesBlock" >> data.json
echo '}}}' >> data.json

curl -su admin:admin -H "X-Requested-By: ambari" -X PUT -d "@./data.json" "http://$ambariServerIp:8080/api/v1/clusters/$clusterName"

rm -f data.json
