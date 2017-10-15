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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.ranger.authorization.accumulo.authorizer.RangerAccumuloPermissionHandler;
import org.apache.ranger.plugin.client.BaseClient;
import org.apache.ranger.plugin.service.ResourceLookupContext;
import org.apache.ranger.plugin.util.TimedEventUtil;

public class ServiceAccumuloClient {

    private static final Logger LOG = Logger.getLogger(ServiceAccumuloClient.class);

    enum RESOURCE_TYPE {

        SYSTEM, TABLE, NAMESPACE
    }

    String serviceName = null;
    private static final String errMessage = " You can still save the repository and start creating "
            + "policies, but you would not be able to use autocomplete for "
            + "resource names. Check server logs for more info.";

    private static final long LOOKUP_TIMEOUT_SEC = 5;

    public ServiceAccumuloClient(String serviceName) {
        this.serviceName = serviceName;

    }

    public HashMap<String, Object> connectionTest() throws Exception {
        String errMsg = errMessage;
        HashMap<String, Object> responseData = new HashMap<String, Object>();

        try {
            getTableList(null);
            // If it doesn't throw exception, then assume the instance is
            // reachable
            String successMsg = "ConnectionTest Successful";
            BaseClient.generateResponseDataMap(true, successMsg,
                    successMsg, null, null, responseData);
        } catch (Exception e) {
            //LOG.error("Error connecting to Accumulo. accumuloClient=" + accumuloClient, e);
            String failureMsg = "Unable to connect to Accumulo instance."
                    + e.getMessage();
            BaseClient.generateResponseDataMap(false, failureMsg,
                    failureMsg + errMsg, null, null, responseData);

        }

        return responseData;
    }

    public List<String> getTableList(List<String> ignoreCollectionList)
            throws Exception {

        List<String> list = new ArrayList<String>();

        return list;
    }

    public List<String> getNamespaceList(List<String> ignoreCollectionList)
            throws Exception {

        List<String> list = new ArrayList<String>();

        return list;
    }

    /**
     * @param serviceName
     * @param context
     * @return
     */
    public List<String> getResources(ResourceLookupContext context) {

        String userInput = context.getUserInput();
        String resource = context.getResourceName();
        Map<String, List<String>> resourceMap = context.getResources();
        List<String> resultList = null;
        List<String> tableList = null;
        List<String> namespaceList = null;

        RESOURCE_TYPE lookupResource = RESOURCE_TYPE.TABLE;

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== getResources() UserInput: \""
                    + userInput + "\" resource : " + resource
                    + " resourceMap: " + resourceMap);
        }

        if (userInput != null && resource != null) {
            if (resourceMap != null && !resourceMap.isEmpty()) {
                tableList = resourceMap.get(RangerAccumuloPermissionHandler.RESOURCE_KEY_TABLE);
                namespaceList = resourceMap.get(RangerAccumuloPermissionHandler.RESOURCE_KEY_NAMESPACE);
            }
            switch (resource.trim().toLowerCase()) {
                case RangerAccumuloPermissionHandler.RESOURCE_KEY_SYSTEM:
                    lookupResource = RESOURCE_TYPE.SYSTEM;
                    break;
                case RangerAccumuloPermissionHandler.RESOURCE_KEY_TABLE:
                    lookupResource = RESOURCE_TYPE.TABLE;
                    break;
                case RangerAccumuloPermissionHandler.RESOURCE_KEY_NAMESPACE:
                    lookupResource = RESOURCE_TYPE.NAMESPACE;
                    break;
                default:
                    break;
            }
        }

        if (userInput != null) {
            try {
                Callable<List<String>> callableObj = null;
                final String userInputFinal = userInput;

                final List<String> finalTableList = tableList;
                final List<String> finalNamespaceList = namespaceList;

                if (lookupResource == RESOURCE_TYPE.TABLE) {
                    // get the collection list for given Input
                    callableObj = new Callable<List<String>>() {
                        @Override
                        public List<String> call() {
                            List<String> retList = new ArrayList<String>();
                            try {
                                List<String> list = getTableList(finalNamespaceList);
                                if (userInputFinal != null
                                        && !userInputFinal.isEmpty()) {
                                    for (String value : list) {
                                        if (value.startsWith(userInputFinal)) {
                                            retList.add(value);
                                        }
                                    }
                                } else {
                                    retList.addAll(list);
                                }
                            } catch (Exception ex) {
                                LOG.error("Error getting collection.", ex);
                            }
                            return retList;
                        }
                    ;
                }  ;
				} else if (lookupResource == RESOURCE_TYPE.NAMESPACE) {
                    callableObj = new Callable<List<String>>() {
                        @Override
                        public List<String> call() {
                            List<String> retList = new ArrayList<String>();
                            try {
                                List<String> list = getNamespaceList(finalTableList);
                                if (userInputFinal != null
                                        && !userInputFinal.isEmpty()) {
                                    for (String value : list) {
                                        if (value.startsWith(userInputFinal)) {
                                            retList.add(value);
                                        }
                                    }
                                } else {
                                    retList.addAll(list);
                                }
                            } catch (Exception ex) {
                                LOG.error("Error getting collection.", ex);
                            }
                            return retList;
                        }
                    ;
                }
                ;
				}
				// If we need to do lookup
				if (callableObj != null) {
                    synchronized (this) {
                        resultList = TimedEventUtil.timedTask(callableObj,
                                LOOKUP_TIMEOUT_SEC, TimeUnit.SECONDS);
                    }
                }
            } catch (Exception e) {
                LOG.error("Unable to get hive resources.", e);
            }
        }

        return resultList;
    }
}
