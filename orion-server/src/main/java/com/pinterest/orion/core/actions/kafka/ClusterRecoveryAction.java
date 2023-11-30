package com.pinterest.orion.core.actions.kafka;

import com.pinterest.orion.core.Cluster;
import com.pinterest.orion.core.Node;
import com.pinterest.orion.core.actions.Action;
import com.pinterest.orion.core.actions.alert.AlertLevel;
import com.pinterest.orion.core.actions.alert.AlertMessage;
import com.pinterest.orion.core.actions.generic.GenericClusterWideAction;
import com.pinterest.orion.server.OrionServer;
import com.pinterest.orion.utils.OrionConstants;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Set;
import java.util.logging.Logger;

public class ClusterRecoveryAction extends GenericClusterWideAction.ClusterAction {

    protected Cluster cluster;
    protected Set<String> candidates;
    protected Set<String> deadBrokers;
    protected Set<String> maybeDeadBrokers;
    protected Set<String> nonExistentBrokers;
    protected Set<String> sensorSet;
    private static final Logger logger = Logger.getLogger(ClusterRecoveryAction.class.getName());

    @Override
    public String getName() {
        return "Cluster Recovery Action";
    }

    @Override
    public void runAction() throws Exception {
        healBrokers(candidates);
    }

    protected void healBroker(String deadBrokerId) throws Exception {
        // This will trigger an action that will attempt to replace the broker ( and first try to restart if agent is still online but Kafka process is down)
        Action brokerRecoveryAction = newBrokerRecoveryAction();
        brokerRecoveryAction.setAttribute(OrionConstants.NODE_ID, deadBrokerId, sensorSet);

        if (nonExistentBrokers.size() == 1) {
            Node existingNode = cluster.getNodeMap().values().iterator().next();
            String extractedName = deriveNonexistentHostname(
                    existingNode.getCurrentNodeInfo().getHostname(),
                    existingNode.getCurrentNodeInfo().getNodeId(),
                    deadBrokerId
            );
            // setting these attributes to indicate that the node doesn't exist in cluster map, and should skip any node-related checks
            brokerRecoveryAction.setAttribute(BrokerRecoveryAction.ATTR_NODE_EXISTS_KEY, false);
            brokerRecoveryAction.setAttribute(BrokerRecoveryAction.ATTR_NONEXISTENT_HOST_KEY, extractedName);
        }

        if (maybeDeadBrokers.size() == 1) {
            // Setting this flag in the action will restart the broker before replacing the broker
            brokerRecoveryAction.setAttribute(BrokerRecoveryAction.ATTR_TRY_TO_RESTART_KEY, true);
            logger.info("Will try to restart node " + deadBrokerId + " before replacing");
        }
        logger.info( "Dispatching BrokerRecoveryAction on " + cluster.getClusterId() + " for node: " +  deadBrokerId);
        getEngine().dispatchChild(this, brokerRecoveryAction);
    }

    protected void healBrokers(Set<String> candidates) throws Exception {
        if (candidates.size() == 1) {
            String deadBrokerId = candidates.iterator().next();
            healBroker(deadBrokerId);
        } else if (candidates.size() > 1){
            cluster.getActionEngine().alert(AlertLevel.HIGH, new AlertMessage(
                    candidates.size() + " brokers on " + cluster.getClusterId() + " are unhealthy",
                    "Brokers " + candidates + " are unhealthy",
                    "orion"
            ));
            OrionServer.metricsCounterInc(
                    "broker.services.unhealthy",
                    new HashMap<String, String>() {{
                        put("clusterId", cluster.getClusterId());
                    }}
            );
            // more than 1 brokers are dead... better alert and have human intervention
            logger.severe("More than one broker is in bad state - dead: " + deadBrokers + " service down: " + maybeDeadBrokers);
        } else {
            logger.info("No candidates for healing");
        }
    }

    public void setCandidates(Set<String> candidates) {
        this.candidates = candidates;
    }

    public void setDeadBrokers(Set<String> deadBrokers) {
        this.deadBrokers = deadBrokers;
    }

    public void setMaybeDeadBrokers(Set<String> maybeDeadBrokers) {
        this.maybeDeadBrokers = maybeDeadBrokers;
    }

    public void setNonExistentBrokers(Set<String> nonExistentBrokers) {
        this.nonExistentBrokers = nonExistentBrokers;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public void setSensorSet(Set<String> sensorSet) {
        this.sensorSet = sensorSet;
    }

    protected static String deriveNonexistentHostname(String existingHostname, String existingId, String nonExistingId) {
        existingHostname = existingHostname.split("\\.", 2)[0]; // sanitize potential suffixes
        int diff = nonExistingId.length() - existingId.length();
        if ( diff > 0 ) {
            existingId = StringUtils.leftPad(existingId, diff, '0');
        } else if (diff < 0) {
            nonExistingId = StringUtils.leftPad(nonExistingId, -diff, '0');
        }

        String ret = existingHostname.replace(existingId, nonExistingId);
        if (ret.equals(existingHostname)) {
            return null;
        }
        return ret;
    }

    protected BrokerRecoveryAction newBrokerRecoveryAction() {
        return new BrokerRecoveryAction();
    }
}
