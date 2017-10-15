/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ranger.services.accumulo.client;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

public class ServiceAccumuloConnectionMgr {

    private static final Logger LOG = Logger
            .getLogger(ServiceAccumuloConnectionMgr.class);

    static public ServiceAccumuloClient getAccumuloClient(String serviceName,
            Map<String, String> configs) throws Exception {
        String url = configs.get("solr.url");
        if (url != null) {

            //SolrClient solrClient = new HttpSolrClient(url);
            ServiceAccumuloClient serviceAccumuloClient = new ServiceAccumuloClient(
                    serviceName);
            return serviceAccumuloClient;
        }
        // TODO: Need to add method to create SolrClient using ZooKeeper for
        // SolrCloud
        throw new Exception("Required properties are not set for "
                + serviceName + ". URL or Zookeeper information not provided.");
    }

    /**
     * @param serviceName
     * @param configs
     * @return
     */
    public static HashMap<String, Object> connectionTest(String serviceName,
            Map<String, String> configs) throws Exception {
        ServiceAccumuloClient serviceAccumuloClient = getAccumuloClient(serviceName,
                configs);
        return serviceAccumuloClient.connectionTest();
    }

}
