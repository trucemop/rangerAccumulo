/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.ranger.plugin.audit;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ranger.audit.model.AuthzAuditEvent;

public class RangerAccumuloAuditHandler extends RangerDefaultAuditHandler {

    Collection<AuthzAuditEvent> auditEvents = new ArrayList<AuthzAuditEvent>();

    public RangerAccumuloAuditHandler() {
    }

    @Override
    public void logAuthzAudit(AuthzAuditEvent auditEvent) {
        auditEvents.add(auditEvent);
    }

    @Override
    public void logAuthzAudits(Collection<AuthzAuditEvent> auditEvents) {
        auditEvents.addAll(auditEvents);
    }

    public void flushAudit() {
        try {
            boolean deniedExists = false;

            AuthzAuditEvent rollup = null;
            StringBuilder sb = new StringBuilder();
            for (AuthzAuditEvent auditEvent : auditEvents) {
                if (rollup == null) {
                    rollup = auditEvent;
                }
                if (auditEvent.getAccessResult() == 0) {
                    deniedExists = true;
                }
                sb.append(auditEvent.getResourcePath());
                sb.append(",");
            }

            if (rollup != null) {
                sb.deleteCharAt(sb.length() - 1);
                rollup.setResourcePath(sb.toString());
                if (deniedExists) {
                    rollup.setPolicyId(-1L);
                    rollup.setAccessResult((short) 0);
                }

                super.logAuthzAudit(rollup);
            }
        } catch (Throwable t) {

        }
    }

    public void flushAudit(boolean forcedAuditResult, long forcePolicyId, String forceResourceType) {
        try {
            short result = (forcedAuditResult) ? (short) 1 : 0;
            for (AuthzAuditEvent auditEvent : auditEvents) {
                auditEvent.setAccessResult(result);
                auditEvent.setPolicyId(forcePolicyId);
                auditEvent.setResourceType(forceResourceType);
                super.logAuthzAudit(auditEvent);
            }
        } catch (Throwable t) {

        }
    }
}
