package org.apache.ranger.authorization.accumulo.authorizer;

import org.apache.accumulo.server.security.handler.KerberosAuthenticator;

public class RangerKerberosAuthenticator extends KerberosAuthenticator {

    @Override
    public boolean userExists(String user) {
        return true;
    }
}
