package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.*;

// Defines a state machine to handle all elections; what happens with the result?
// we put the result somewhere...
// rename to ongoing election or something...
public class OngoingElectionStateMachine {
    private final long gatherDeadline;
    private final Map<TopicIdPartition, TopicPartitionElectionState> topicPartitionMachineMap;
    private final ElectionStateMachineStore store;

    OngoingElectionStateMachine(List<TopicIdPartition> topics, long gatherDeadline) {
        this.gatherDeadline = gatherDeadline;
        this.store = new ElectionStateMachineStore();
        var entries = topics.stream().map(tip -> Map.entry(tip, new TopicPartitionElectionState()));
        // TODO figure out how to fix these compilation warnings
        this.topicPartitionMachineMap = Map.ofEntries(
                entries.<Map.Entry>toArray(Map.Entry[]::new)
        );
    }

    public void updateElectionStates(Node node, GetReplicaLogInfoResponseData responseData) {
        for (GetReplicaLogInfoResponseData.TopicPartitionLogInfo tp: responseData.topicPartitionLogInfoList()) {
            for (GetReplicaLogInfoResponseData.PartitionLogInfo info: tp.partitionLogInfo()) {
                TopicIdPartition tip = new TopicIdPartition(tp.topicId(), info.partition());
                store.add(tip, node.id(), ElectionStateMachineStore.EpochOffset.from(info));
            }
        }
    }

    public void completeElection(UncleanRecoveryResult result) {
        if (result.error == ApiError.NONE) {
            topicPartitionMachineMap.get(result).finish();
        } else {
            topicPartitionMachineMap.get(result).fail(result.error);
        }
    }

    public ElectionStateMachineStore store() {
        return store;
    }

    public Set<TopicIdPartition> topics() {
        return topicPartitionMachineMap.keySet();
    }

    public long gatherDeadline() {
        return gatherDeadline;
    }
}
