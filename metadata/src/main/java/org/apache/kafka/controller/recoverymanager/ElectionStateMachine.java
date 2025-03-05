package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

enum ElectionState {
    PENDING,
    GATHERING_LOG_INFO,
    READY_FOR_ELECTION,
    CONTROLLER_WRITE,
    COMPLETED,
    FAILED;

    public boolean isTerminalState() {
        return this == FAILED || this == COMPLETED;
    }
}


public class ElectionStateMachine {
    private ElectionState electionState;
    private List<LogResult> logInfoQueue;

    public ElectionStateMachine() {
        this.logInfoQueue = new ArrayList<LogResult>();
        this.electionState = ElectionState.PENDING;
    }

    public void addResult(LogResult result) {
        logInfoQueue.add(result);
    }

    public ElectionState electionState() {
        return electionState;
    }

    static class LogResult {
        private final TopicIdPartition topicIdPartition;
        private final int replicaId;
        private final GetReplicaLogInfoResponseData.PartitionLogInfo partitionLogInfo;

        LogResult(TopicIdPartition topicIdPartition, int replicaId, GetReplicaLogInfoResponseData.PartitionLogInfo partitionLogInfo) {
            this.topicIdPartition = topicIdPartition;
            this.replicaId = replicaId;
            this.partitionLogInfo = partitionLogInfo;
        }
    }



    static class Dimension {
        public final Node node;
        public final TopicIdPartition topicPartition;

        public Dimension(Node node, TopicIdPartition topicPartition) {
            this.node = node;
            this.topicPartition = topicPartition;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Dimension dimension = (Dimension) o;
            return Objects.equals(node, dimension.node) && Objects.equals(topicPartition, dimension.topicPartition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(node, topicPartition);
        }
    }
}
