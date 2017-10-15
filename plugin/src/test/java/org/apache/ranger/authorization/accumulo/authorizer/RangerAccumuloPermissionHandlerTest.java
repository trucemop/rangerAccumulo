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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.impl.Namespaces;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.server.zookeeper.ZooCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.junit.Assert.*;
import org.mockito.Matchers;

public class RangerAccumuloPermissionHandlerTest {

    protected ServicePolicies adminHasAllPolicies;
    protected ServicePolicies joeHasSomePolicies;
    protected RangerAccumuloPermissionHandler basicRap;

    public RangerAccumuloPermissionHandlerTest() throws Exception {
        File file = new File(getClass().getResource(getClass().getSimpleName() + "/" + "adminHasAll_accumulo.json").getPath());

        Gson gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
        Reader reader = new FileReader(file);
        adminHasAllPolicies = gson.fromJson(reader, ServicePolicies.class);

        file = new File(getClass().getResource(getClass().getSimpleName() + "/" + "joeHasSome_accumulo.json").getPath());

        gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
        reader = new FileReader(file);
        joeHasSomePolicies = gson.fromJson(reader, ServicePolicies.class);

        ZooCache mockZc = (ZooCache) mock(ZooCache.class);
        when(mockZc.get(Matchers.anyString())).thenReturn(null);
        basicRap = new RangerAccumuloPermissionHandler(mockZc);
        basicRap.initialize("accumulo", true);
    }

    @Test
    public void testAdminHasAllSystemPermissions() throws Exception {

        RangerAccumuloPermissionHandler.accumuloPlugin.setPolicies(adminHasAllPolicies);

        assertTrue(basicRap.hasSystemPermission("admin@EXAMPLE.COM", SystemPermission.GRANT));
        assertTrue(basicRap.hasSystemPermission("admin/fqdn.server.net@EXAMPLE.COM", SystemPermission.CREATE_TABLE));
        assertTrue(basicRap.hasSystemPermission("admin", SystemPermission.DROP_TABLE));
        assertTrue(basicRap.hasSystemPermission("admin", SystemPermission.ALTER_TABLE));
        assertTrue(basicRap.hasSystemPermission("admin", SystemPermission.CREATE_USER));
        assertTrue(basicRap.hasSystemPermission("admin", SystemPermission.DROP_USER));
        assertTrue(basicRap.hasSystemPermission("admin", SystemPermission.ALTER_USER));
        assertTrue(basicRap.hasSystemPermission("admin", SystemPermission.SYSTEM));
        assertTrue(basicRap.hasSystemPermission("admin", SystemPermission.CREATE_NAMESPACE));
        assertTrue(basicRap.hasSystemPermission("admin", SystemPermission.DROP_NAMESPACE));
        assertTrue(basicRap.hasSystemPermission("admin", SystemPermission.ALTER_NAMESPACE));
        assertTrue(basicRap.hasSystemPermission("admin", SystemPermission.OBTAIN_DELEGATION_TOKEN));
    }

    @Test
    public void testUserDoesNotHaveSystemPermissions() throws Exception {

        RangerAccumuloPermissionHandler.accumuloPlugin.setPolicies(adminHasAllPolicies);
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.GRANT));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.CREATE_TABLE));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.DROP_TABLE));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.ALTER_TABLE));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.CREATE_USER));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.DROP_USER));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.ALTER_USER));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.SYSTEM));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.CREATE_NAMESPACE));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.DROP_NAMESPACE));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.ALTER_NAMESPACE));
        assertFalse(basicRap.hasSystemPermission("joe", SystemPermission.OBTAIN_DELEGATION_TOKEN));
    }

    @Test
    public void testAdminHasAllTablePermission() throws Exception {

        RangerAccumuloPermissionHandler.accumuloPlugin.setPolicies(adminHasAllPolicies);

        assertTrue(basicRap.hasTablePermission("admin", "test", TablePermission.READ));
        assertTrue(basicRap.hasTablePermission("admin", "test", TablePermission.WRITE));
        assertTrue(basicRap.hasTablePermission("admin", "test", TablePermission.BULK_IMPORT));
        assertTrue(basicRap.hasTablePermission("admin", "test", TablePermission.ALTER_TABLE));
        assertTrue(basicRap.hasTablePermission("admin", "test", TablePermission.GRANT));
        assertTrue(basicRap.hasTablePermission("admin", "test", TablePermission.DROP_TABLE));
    }

    @Test
    public void testUserDoesNotHaveTablePermissions() throws Exception {

        RangerAccumuloPermissionHandler.accumuloPlugin.setPolicies(adminHasAllPolicies);
        assertFalse(basicRap.hasTablePermission("joe", "test", TablePermission.READ));
        assertFalse(basicRap.hasTablePermission("joe", "test", TablePermission.WRITE));
        assertFalse(basicRap.hasTablePermission("joe", "test", TablePermission.BULK_IMPORT));
        assertFalse(basicRap.hasTablePermission("joe", "test", TablePermission.ALTER_TABLE));
        assertFalse(basicRap.hasTablePermission("joe", "test", TablePermission.GRANT));
        assertFalse(basicRap.hasTablePermission("joe", "test", TablePermission.DROP_TABLE));
    }

    @Test
    public void testAdminHasAllNamespacePermissions() throws Exception {

        RangerAccumuloPermissionHandler.accumuloPlugin.setPolicies(adminHasAllPolicies);
        assertTrue(basicRap.hasNamespacePermission("admin", "testNamespace", NamespacePermission.READ));
        assertTrue(basicRap.hasNamespacePermission("admin", "testNamespace", NamespacePermission.WRITE));
        assertTrue(basicRap.hasNamespacePermission("admin", "testNamespace", NamespacePermission.ALTER_NAMESPACE));
        assertTrue(basicRap.hasNamespacePermission("admin", "testNamespace", NamespacePermission.GRANT));
        assertTrue(basicRap.hasNamespacePermission("admin", "testNamespace", NamespacePermission.ALTER_TABLE));
        assertTrue(basicRap.hasNamespacePermission("admin", "testNamespace", NamespacePermission.CREATE_TABLE));
        assertTrue(basicRap.hasNamespacePermission("admin", "testNamespace", NamespacePermission.DROP_TABLE));
        assertTrue(basicRap.hasNamespacePermission("admin", "testNamespace", NamespacePermission.BULK_IMPORT));
        assertTrue(basicRap.hasNamespacePermission("admin", "testNamespace", NamespacePermission.DROP_NAMESPACE));

    }

    @Test
    public void testUserDoesNotHaveNamespacePermissions() throws Exception {

        RangerAccumuloPermissionHandler.accumuloPlugin.setPolicies(adminHasAllPolicies);
        assertFalse(basicRap.hasNamespacePermission("joe", "testNamespace", NamespacePermission.READ));
        assertFalse(basicRap.hasNamespacePermission("joe", "testNamespace", NamespacePermission.WRITE));
        assertFalse(basicRap.hasNamespacePermission("joe", "testNamespace", NamespacePermission.ALTER_NAMESPACE));
        assertFalse(basicRap.hasNamespacePermission("joe", "testNamespace", NamespacePermission.GRANT));
        assertFalse(basicRap.hasNamespacePermission("joe", "testNamespace", NamespacePermission.ALTER_TABLE));
        assertFalse(basicRap.hasNamespacePermission("joe", "testNamespace", NamespacePermission.CREATE_TABLE));
        assertFalse(basicRap.hasNamespacePermission("joe", "testNamespace", NamespacePermission.DROP_TABLE));
        assertFalse(basicRap.hasNamespacePermission("joe", "testNamespace", NamespacePermission.BULK_IMPORT));
        assertFalse(basicRap.hasNamespacePermission("joe", "testNamespace", NamespacePermission.DROP_NAMESPACE));

    }

    @Test
    public void testTableAndNamespacePermissions() throws Exception {

        String instanceId = "instanceId";
        String namespaceName1 = Namespaces.DEFAULT_NAMESPACE_ID;
        String tableId1 = "3";
        String tableName1 = "bar";

        String namespaceId2 = "r";
        String namespaceName2 = "foo";
        String tableId2 = "4";
        String tableName2 = "bar";

        String namespaceId3 = "q";
        String namespaceName3 = "what";

        String tableId3 = "6";
        String tableName3 = "bar";

        ZooCache mockZc = (ZooCache) mock(ZooCache.class);

        basicRap = new RangerAccumuloPermissionHandler(mockZc);

        basicRap.initialize(instanceId, true);

        when(mockZc.get(basicRap.ZKTablePath + "/" + tableId1 + Constants.ZTABLE_NAME)).thenReturn(tableName1.getBytes());
        when(mockZc.get(basicRap.ZKTablePath + "/" + tableId1 + Constants.ZTABLE_NAMESPACE)).thenReturn(namespaceName1.getBytes());

        when(mockZc.get(basicRap.ZKTablePath + "/" + tableId2 + Constants.ZTABLE_NAME)).thenReturn(tableName2.getBytes());
        when(mockZc.get(basicRap.ZKTablePath + "/" + tableId2 + Constants.ZTABLE_NAMESPACE)).thenReturn(namespaceId2.getBytes());
        when(mockZc.get(basicRap.ZKNamespacePath + "/" + namespaceId2 + Constants.ZNAMESPACE_NAME)).thenReturn(namespaceName2.getBytes());

        when(mockZc.get(basicRap.ZKTablePath + "/" + tableId3 + Constants.ZTABLE_NAME)).thenReturn(tableName3.getBytes());
        when(mockZc.get(basicRap.ZKTablePath + "/" + tableId3 + Constants.ZTABLE_NAMESPACE)).thenReturn(namespaceId3.getBytes());
        when(mockZc.get(basicRap.ZKNamespacePath + "/" + namespaceId3 + Constants.ZNAMESPACE_NAME)).thenReturn(namespaceName3.getBytes());

        RangerAccumuloPermissionHandler.accumuloPlugin.setPolicies(joeHasSomePolicies);
        assertTrue(basicRap.hasTablePermission("joe", tableId1, TablePermission.READ));

        assertFalse(basicRap.hasTablePermission("joe", tableId2, TablePermission.READ));

        assertTrue(basicRap.hasTablePermission("joe", tableId3, TablePermission.READ));

    }

    @Test
    public void testAuthToLocal() throws Exception {

        RangerAccumuloPermissionHandler.accumuloPlugin.setPolicies(adminHasAllPolicies);
        System.setProperty("hadoop.security.auth_to_local", "RULE:[1:$1@$0](admin@EXAMPLE.COM)s/.*/admin/");
        assertTrue(basicRap.hasNamespacePermission("admin/fqdn.stuff.com@REALM.COM", "testNamespace", NamespacePermission.READ));
    }

    private static class DummyLoginConfiguration extends javax.security.auth.login.Configuration {

        @Override
        public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            throw new RuntimeException("UGI is not using its own security config");
        }
    }

}
