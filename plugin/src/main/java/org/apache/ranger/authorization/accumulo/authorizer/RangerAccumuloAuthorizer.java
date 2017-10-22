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
package org.apache.ranger.authorization.accumulo.authorizer;

import com.google.common.base.Charsets;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.KerberosAuthorizor;
import org.apache.accumulo.server.security.handler.PermissionHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.audit.RangerAccumuloAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.services.accumulo.RangerAccumuloPlugin;

public class RangerAccumuloAuthorizer extends KerberosAuthorizor {

    private static final Log logger = LogFactory
            .getLog(RangerAccumuloAuthorizer.class);
    private String ipAddress;

    protected RangerAccumuloUgi rau = new RangerAccumuloUgi();

    protected static volatile RangerAccumuloPlugin accumuloPlugin = null;

    public static final String RESOURCE_KEY_AUTHORIZATION = "authorization";
    public static final String RESOURCE_KEY_ALL = "all";

    public static final String ACCESS_TYPE = "AUTH";

    public RangerAccumuloAuthorizer() {

    }

    @Override
    public void initialize(String instanceId, boolean initialize) {
        String appType = System.getProperty("app");
        try {
            ipAddress = Inet4Address.getLocalHost().getHostAddress();
        } catch (UnknownHostException ex) {
        }
        if (appType == null) {
            appType = "accumulo";
        }
        logger.info("AppType: " + appType);
        try {

            accumuloPlugin = new RangerAccumuloPlugin("accumulo", appType);
            accumuloPlugin.init();
        } catch (Throwable t) {
            logger.fatal("Error creating and initializing RangerBasePlugin()", t);
        }
    }

    @Override
    public Authorizations getCachedUserAuthorizations(String user) {

        UserGroupInformation ugi = user == null ? null : rau.createRemoteUser(user);
        String name = ugi == null ? user : rau.getShortUserName(ugi);
        Set<String> groupSet = rau.getGroupNames(ugi);
        String groups = groupSet.toString();

        Authorizations authorizations = Authorizations.EMPTY;
        List<byte[]> authorizationList = new ArrayList<>();
        List<RangerPolicy> policyList = accumuloPlugin.getPolicyEngine().getAllowedPolicies(name, groupSet, ACCESS_TYPE);

        for (RangerPolicy policy : policyList) {
            if (policy.getIsEnabled()) {
                for (RangerPolicy.RangerPolicyItem policyItem : policy.getPolicyItems()) {
                    if (policyItem.getUsers().contains(name)) {
                        for (Map.Entry<String, RangerPolicy.RangerPolicyResource> resource : policy.getResources().entrySet()) {
                            if (resource.getKey().equals(RESOURCE_KEY_AUTHORIZATION) || resource.getKey().equals(RESOURCE_KEY_ALL)) {
                                for (String value : resource.getValue().getValues()) {
                                    authorizationList.add(value.getBytes());
                                }
                            }
                        }
                    } else {
                        for (String group : groupSet) {
                            if (policyItem.getGroups().contains(group)) {
                                for (Map.Entry<String, RangerPolicy.RangerPolicyResource> resource : policy.getResources().entrySet()) {
                                    if (resource.getKey().equals(RESOURCE_KEY_AUTHORIZATION) || resource.getKey().equals(RESOURCE_KEY_ALL)) {
                                        for (String value : resource.getValue().getValues()) {
                                            authorizationList.add(value.getBytes());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        List<String> authStringList = new ArrayList<>();
        for (byte[] auth : authorizationList) {
            authStringList.add(new String(auth, Charsets.UTF_8));
        }
        logger.info("getCachedUserAuthorizations for user: " + name + " with groups: " + groups + " was: " + authStringList.toString());
        if (authorizationList.size() > 0) {
            authorizations = new Authorizations(authorizationList);
        }

        return authorizations;
    }

    @Override
    public boolean validSecurityHandlers(Authenticator auth, PermissionHandler pm) {
        return auth instanceof RangerKerberosAuthenticator && pm instanceof RangerAccumuloPermissionHandler;
    }

    @Override
    public void initializeSecurity(TCredentials itw, String rootuser) throws AccumuloSecurityException {
    }

    @Override
    public void initUser(String user) throws AccumuloSecurityException {
    }

    @Override
    public void dropUser(String user) throws AccumuloSecurityException {
    }

    @Override
    public void changeAuthorizations(String user, Authorizations authorizations) throws AccumuloSecurityException {
    }

    @Override
    public boolean isValidAuthorizations(String user, List<ByteBuffer> auths) throws AccumuloSecurityException {

        UserGroupInformation ugi = user == null ? null : rau.createRemoteUser(user);
        String name = ugi == null ? user : rau.getShortUserName(ugi);
        Set<String> groupSet = rau.getGroupNames(ugi);
        String groups = groupSet.toString();

        List<String> authStrings = new ArrayList<>();
        List<RangerAccessRequest> requestList = new ArrayList<>();
        RangerAccumuloAuditHandler auditHandler = new RangerAccumuloAuditHandler();

        for (ByteBuffer auth : auths) {
            String authString = new String(auth.array(), Charsets.UTF_8);
            RangerAccessRequestImpl request = new RangerAccessRequestImpl();
            request.setAccessType(ACCESS_TYPE);
            request.setAction(ACCESS_TYPE);
            request.setUser(name);
            request.setUserGroups(groupSet);
            request.setClientIPAddress(ipAddress);
            RangerAccessResourceImpl resource = new RangerAccessResourceImpl();
            resource.setValue(RESOURCE_KEY_AUTHORIZATION, authString);
            resource.setOwnerUser(user);
            request.setResource(resource);
            requestList.add(request);
            authStrings.add(authString);
        }

        logger.info("isValidAuthorizations for user: " + user + " with groups: " + groups + " and auth request list: " + authStrings.toString());

        Collection<RangerAccessResult> results = null;

        try {
            results = accumuloPlugin.isAccessAllowed(requestList, auditHandler, true);
        } finally {
            auditHandler.flushAudit();
        }

        if (results != null) {
            for (RangerAccessResult result : results) {
                if (!result.getIsAllowed()) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }

    }

}
