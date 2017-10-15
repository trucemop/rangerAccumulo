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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.ranger.plugin.util.ServicePolicies;
import org.junit.Test;
import static org.junit.Assert.*;

public class RangerAccumuloAuthorizerTest {

    protected ServicePolicies authorizations;

    protected RangerAccumuloAuthorizer basicRaa;

    public RangerAccumuloAuthorizerTest() throws FileNotFoundException {
        File file = new File(getClass().getResource(getClass().getSimpleName() + "/" + "authorizations.json").getPath());

        Gson gson = new GsonBuilder().setDateFormat("yyyyMMdd-HH:mm:ss.SSS-Z").setPrettyPrinting().create();
        Reader reader = new FileReader(file);
        authorizations = gson.fromJson(reader, ServicePolicies.class);

        basicRaa = new RangerAccumuloAuthorizer();
        basicRaa.initialize("accumulo", true);
    }

    @Test
    public void testIsValidAuthorizations() throws AccumuloSecurityException {
        RangerAccumuloAuthorizer.accumuloPlugin.setPolicies(authorizations);
        List<ByteBuffer> authList = new ArrayList<>();
        assertTrue(basicRaa.isValidAuthorizations("noOne", authList));

        authList.add(ByteBuffer.wrap("crazy".getBytes()));

        assertFalse(basicRaa.isValidAuthorizations("noOne", authList));
        assertFalse(basicRaa.isValidAuthorizations("joe", authList));

        authList.add(ByteBuffer.wrap("foo".getBytes()));
        assertFalse(basicRaa.isValidAuthorizations("joe", authList));

        authList.clear();
        authList.add(ByteBuffer.wrap("foo".getBytes()));
        assertTrue(basicRaa.isValidAuthorizations("joe", authList));
        assertTrue(basicRaa.isValidAuthorizations("frank", authList));

        authList.add(ByteBuffer.wrap("bar".getBytes()));
        authList.add(ByteBuffer.wrap("what".getBytes()));
        assertTrue(basicRaa.isValidAuthorizations("joe", authList));
        assertFalse(basicRaa.isValidAuthorizations("frank", authList));
    }

    @Test
    public void testGetCachedUserAuthorizations() {
        Authorizations authzs = new Authorizations("foo", "bar", "what");
        RangerAccumuloAuthorizer.accumuloPlugin.setPolicies(authorizations);
        assertEquals("Should be equal", authzs, basicRaa.getCachedUserAuthorizations("joe"));
        authzs = new Authorizations("foo");
        assertEquals("Should be equal", authzs, basicRaa.getCachedUserAuthorizations("frank"));
    }

}
