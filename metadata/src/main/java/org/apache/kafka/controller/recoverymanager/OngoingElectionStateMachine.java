package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.*;

// Defines a state machine to handle all elections; what happens with the result?
// we put the result somewhere...
// rename to ongoing election or something...
public class OngoingElectionStateMachine {
    enum State {
        GATHERING_LOGS,
        AWAITING_CONTROLLER,
        COMPLETE,
    }
    private State state = State.GATHERING_LOGS;
    private final Map<TopicIdPartition, TopicPartitionElectionState> topicPartitionMachineMap;
    private final ElectionStateMachineStore store;

    OngoingElectionStateMachine(List<TopicIdPartition> topics) {
        this.store = new ElectionStateMachineStore();
        // TODO figure out how to make this immutable
        this.topicPartitionMachineMap = new HashMap<>();
        for (TopicIdPartition tip : topics) {
            this.topicPartitionMachineMap.put(tip, new TopicPartitionElectionState());
        }
    }

    public void updateElectionStates(Node node, GetReplicaLogInfoResponseData responseData) {
        assert state == State.GATHERING_LOGS;
        for (GetReplicaLogInfoResponseData.TopicPartitionLogInfo tp: responseData.topicPartitionLogInfoList()) {
            for (GetReplicaLogInfoResponseData.PartitionLogInfo info: tp.partitionLogInfo()) {
                TopicIdPartition tip = new TopicIdPartition(tp.topicId(), info.partition());
                store.add(tip, node.id(), ElectionStateMachineStore.EpochOffset.from(info));
            }
        }
    }

    public void completeElection(UncleanRecoveryResult result) {
        assert state == State.AWAITING_CONTROLLER;
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

    public void finishGathering() {
        this.state = State.AWAITING_CONTROLLER;
    }

    public void finish() {
        this.state = State.COMPLETE;
    }
}
