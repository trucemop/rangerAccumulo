package org.apache.ranger.plugin.policyengine;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ranger.plugin.util.ServicePolicies;

public class RangerAccumuloPolicyEngineImpl extends RangerPolicyEngineImpl {

    private static final Log LOG = LogFactory.getLog(RangerPolicyEngineImpl.class);

    public RangerAccumuloPolicyEngineImpl(String appId, ServicePolicies servicePolicies, RangerPolicyEngineOptions options) {
        super(appId, servicePolicies, options);
    }

    @Override
    public Collection<RangerAccessResult> isAccessAllowed(Collection<RangerAccessRequest> requests, RangerAccessResultProcessor resultProcessor) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + requests + ")");
        }

        Collection<RangerAccessResult> ret = new ArrayList<RangerAccessResult>();

        if (requests != null) {
            for (RangerAccessRequest request : requests) {

                RangerAccessResult result = isAccessAllowedNoAudit(request);
                result.setIsAudited(true);
                ret.add(result);
            }
        }

        if (resultProcessor != null) {
            resultProcessor.processResults(ret);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + requests + "): " + ret);
        }

        return ret;
    }

    @Override
    public RangerAccessResult isAccessAllowed(RangerAccessRequest request, RangerAccessResultProcessor resultProcessor) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("==> RangerPolicyEngineImpl.isAccessAllowed(" + request + ")");
        }

        RangerAccessResult ret = isAccessAllowedNoAudit(request);

        if (resultProcessor != null) {

            ret.setIsAudited(true);
            resultProcessor.processResult(ret);

        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("<== RangerPolicyEngineImpl.isAccessAllowed(" + request + "): " + ret);
        }

        return ret;
    }

}
