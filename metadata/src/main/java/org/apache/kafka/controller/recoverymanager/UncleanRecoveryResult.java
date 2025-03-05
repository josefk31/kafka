package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.common.requests.ApiError;
import org.apache.kafka.server.common.TopicIdPartition;

public class UncleanRecoveryResult {
     final public ApiError error;
     final public TopicIdPartition topicPartition;

    public UncleanRecoveryResult(ApiError error, TopicIdPartition topicPartition) {
        this.error = error;
        this.topicPartition = topicPartition;
    }
}
