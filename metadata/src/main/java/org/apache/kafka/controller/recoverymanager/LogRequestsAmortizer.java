package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.GetReplicaLogInfoRequest;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Groups GetReplicaLogInfo requests into groups by broker in order to minimize the number of requests sent to each.
 */
class LogRequestsAmortizer {
    private final Map<Integer, List<RecoveryLogRequestWork.Builder>> builderMap;
    private final Map<Integer, Node> brokerNodeMap;

    public LogRequestsAmortizer() {
        builderMap = new HashMap<>();
        brokerNodeMap = new HashMap<>();
    }

    public void addTopic(int brokerId, TopicIdPartition tp) {
        RecoveryLogRequestWork.Builder builder;
        if (builderMap.containsKey(brokerId)) {
            builder = builderMap.get(brokerId).getLast();
            builder.addTopic(tp);
        } else {
            var builders = new ArrayList<RecoveryLogRequestWork.Builder>();
            builder = new RecoveryLogRequestWork.Builder()
                    .setBrokerId(brokerId)
                    .addTopic(tp);
            builders.add(builder);
            builderMap.put(brokerId, builders);
        }
        if (builder.getTopicPartitionsCount() >= GetReplicaLogInfoRequest.MAX_PARTITIONS_PER_REQUEST) {
            builderMap.get(brokerId).add(new RecoveryLogRequestWork.Builder().setBrokerId(brokerId));
        }
    }

    public void setNode(int brokerId, Node node) {
        brokerNodeMap.putIfAbsent(brokerId, node);
    }

    public List<RecoveryLogRequestWork> buildAll(ElectionDriver driver, ElectionStateMachine machine) {
        // Be sure to set all of the chosen nodes for each broker
        brokerNodeMap.forEach((brokerId, node) -> {
            builderMap.get(brokerId).forEach(builder -> builder.setNode(node));
        });
        // Build all of the needed requests
        return builderMap.values()
                .stream()
                .flatMap(List::stream)
                .map(builder -> builder.setDriver(driver).setOngoingElectionStateMachine(machine))
                .map(RecoveryLogRequestWork.Builder::build)
                .toList();
    }
}
