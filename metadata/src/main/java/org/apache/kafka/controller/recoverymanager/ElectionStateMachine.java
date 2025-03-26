package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.*;

// Defines a state machine to handle all elections; what happens with the result?
// we put the result somewhere...
// rename to ongoing election or something...
public class ElectionStateMachine {
    enum ElectionResultState {
        ONGOING,
        SUCCESS,
        FAILED,
    }

    private final LogLengthInfoStore store;
    private final Map<TopicIdPartition, ElectionResultState> topicElectionResults;
    private int brokerGatheringCount = 0;
    private boolean gatheringStopped = false;
    private boolean hasCompletedAllElections = false;

    ElectionStateMachine(List<TopicIdPartition> topicIdPartitions) {
        this.store = new LogLengthInfoStore();
        this.topicElectionResults = new HashMap<>();
        for (TopicIdPartition topicIdPartition : topicIdPartitions) {
            topicElectionResults.put(topicIdPartition, ElectionResultState.ONGOING);
        }
    }

    public void updateElectionStates(int brokerId, GetReplicaLogInfoResponseData responseData) {
        for (GetReplicaLogInfoResponseData.TopicPartitionLogInfo tp: responseData.topicPartitionLogInfoList()) {
            for (GetReplicaLogInfoResponseData.PartitionLogInfo info: tp.partitionLogInfo()) {
                if (info.errorCode() == Errors.NONE.code()) {
                    TopicIdPartition tip = new TopicIdPartition(tp.topicId(), info.partition());
                    store.add(tip, brokerId, LogLengthInfoStore.EpochOffset.from(info));
                }
            }
        }
    }

    public void completeElection(List<UncleanRecoveryResult> results) {
        this.hasCompletedAllElections = true;
        if (results == null || results.isEmpty()) {
            return;
        }
        for (UncleanRecoveryResult result : results) {
            assert topicElectionResults.get(result.topicPartition) != null;
            if (result.error.is(Errors.NONE)) {
                topicElectionResults.put(result.topicPartition, ElectionResultState.SUCCESS);
            } else {
                topicElectionResults.put(result.topicPartition, ElectionResultState.FAILED);
            }
        }
    }

    public void setBrokerGatheringCount(int brokerGatheringCount) {
        this.brokerGatheringCount = brokerGatheringCount;
    }

    public LogLengthInfoStore store() {
        return store;
    }

    public Set<TopicIdPartition> topics() {
        return topicElectionResults.keySet();
    }

    public void finishedRequestsForBroker() {
        brokerGatheringCount--;
        assert brokerGatheringCount > 0;
    }

    public void stopGathering() {
        gatheringStopped = true;
    }

    public boolean completedGathering() {
        return brokerGatheringCount == 0 || gatheringStopped;
    }

    public boolean hasCompletedAllElections() {
        return hasCompletedAllElections;
    }
}
