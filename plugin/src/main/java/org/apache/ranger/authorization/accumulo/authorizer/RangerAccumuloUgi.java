package org.apache.ranger.authorization.accumulo.authorizer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.apache.hadoop.security.UserGroupInformation;
import java.util.List;
import java.util.Set;

public class RangerAccumuloUgi {

    public UserGroupInformation createRemoteUser(String user) {
        return UserGroupInformation.createRemoteUser(user);
    }

    public Set<String> getGroupNames(UserGroupInformation ugi) {
        Set<String> set = new HashSet<>();
        Collections.addAll(set, ugi.getGroupNames());
        return set;
    }

    public String getShortUserName(UserGroupInformation ugi) {
        return ugi.getShortUserName();
    }
}
