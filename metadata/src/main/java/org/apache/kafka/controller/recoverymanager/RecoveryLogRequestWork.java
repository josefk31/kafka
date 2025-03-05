package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.GetReplicaLogInfoRequestData;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetReplicaLogInfoRequest;
import org.apache.kafka.common.requests.GetReplicaLogInfoResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.*;

// TODO add a retry count...
// TODO error handling
class RecoveryLogRequestWork implements RequestCompletionHandler {
    public final AbstractRequest.Builder<?> builder;
    private final ElectionDriver driver;
    private final ElectionStateMachine machine;
    public final Node node;
    public final int brokerId;
    public final int retryCount;

    public RecoveryLogRequestWork(AbstractRequest.Builder<?> builder,
                                  ElectionDriver driver,
                                  ElectionStateMachine machine,
                                  Node node,
                                  int brokerId,
                                  int retryCount) {
        this.builder = builder;
        this.driver = driver;
        this.machine = machine;
        this.node = node;
        this.brokerId = brokerId;
        this.retryCount = retryCount;
    }

    @Override
    public void onComplete(ClientResponse clientResponse) {
        driver.processReplicaLogResponse(machine, brokerId, Result.fromPreviousWork(this, clientResponse));
    }

    static class Builder {
        private Map<Uuid, List<Integer>> partitionForTopic;
        private Node node;
        private int brokerId;
        private ElectionDriver driver;
        private ElectionStateMachine ongoing;
        private int topicPartitionsCount;

        Builder() {
            partitionForTopic = new HashMap<>();
        }

        public Builder addTopic(TopicIdPartition tp) {
            partitionForTopic.putIfAbsent(tp.topicId(), Collections.emptyList());
            partitionForTopic.get(tp.topicId()).add(tp.partitionId());
            topicPartitionsCount += tp.partitionId();
            return this;
        }

        public int getTopicPartitionsCount() {
            return topicPartitionsCount;
        }

        public Builder setNode(Node node) {
            if (this.node == null) {
                this.node = node;
            }
            return this;
        }

        public Builder setBrokerId(int brokerId) {
            this.brokerId = brokerId;
            return this;
        }

        public Builder setDriver(ElectionDriver driver) {
            this.driver = driver;
            return this;
        }

        public Builder setOngoingElectionStateMachine(ElectionStateMachine ongoing) {
            this.ongoing = ongoing;
            return this;
        }

        public RecoveryLogRequestWork build() {
            GetReplicaLogInfoRequestData requestData = new GetReplicaLogInfoRequestData();
            // TODO why broker ID again??
            for (Map.Entry<Uuid, List<Integer>> entry : partitionForTopic.entrySet()) {
                requestData.topicPartitions().add(new GetReplicaLogInfoRequestData.TopicPartitions()
                        .setTopicId(entry.getKey())
                        .setPartitions(entry.getValue()));
            }
            AbstractRequest.Builder<?> requestBuilder = new GetReplicaLogInfoRequest.Builder(requestData);
            return new RecoveryLogRequestWork(requestBuilder, driver, ongoing, node, brokerId, 0);
        }
    }

    static class Result {
        public static final long BACKOFF_MAX_MS = 3_000;
        public static final int BACKOFF_INITIAL_INTERVAL_MS = 0;
        public static final int BACKOFF_MULTIPLIER_MS = 50;
        public static final double BACKOFF_JITTER = 0.3;

        public final RecoveryLogRequestWork nextRequest;
        // TODO make optional...
        public final GetReplicaLogInfoResponseData responseData;
        public final long backoffMs;
        public final boolean succeeded;

        Result(RecoveryLogRequestWork nextRequest, GetReplicaLogInfoResponseData responseData, long backoffMs, boolean succeeded) {
            this.nextRequest = nextRequest;
            this.responseData = responseData;
            this.backoffMs = backoffMs;
            this.succeeded = succeeded;
        }

        Result(GetReplicaLogInfoResponseData responseData) {
            this.nextRequest = null;
            this.responseData = responseData;
            this.backoffMs = -1;
            this.succeeded = true;
        }

        static Result fromPreviousWork(RecoveryLogRequestWork previousWork, ClientResponse clientResponse) {
            // Transient errors; retry them
            if (clientResponse.wasDisconnected() || clientResponse.wasTimedOut() || clientResponse.authenticationException() != null) {
                // see if we need configuration for this...
                var backoff = new ExponentialBackoff(
                        BACKOFF_INITIAL_INTERVAL_MS,
                        BACKOFF_MULTIPLIER_MS,
                        BACKOFF_MAX_MS,
                        BACKOFF_JITTER);
                var nextRetryCount = previousWork.retryCount + 1;
                var nextWork = new RecoveryLogRequestWork(
                        previousWork.builder,
                        previousWork.driver,
                        previousWork.machine,
                        previousWork.node,
                        previousWork.brokerId,
                        nextRetryCount
                );
                return new Result(nextWork, null, backoff.backoff(nextRetryCount), false);
            }
            GetReplicaLogInfoResponse logInfoResponse = (GetReplicaLogInfoResponse) clientResponse.responseBody();
            GetReplicaLogInfoResponseData responseData = logInfoResponse.data();
            // Optimistic case; send back a succesful response
            if (logInfoResponse.errorCounts().isEmpty()) {
                return new Result(responseData);
            }
            // in this case we have partial failure of GetReplicaLogInfoRequest; retry topic/partitions which did not succeed.
            GetReplicaLogInfoRequestData requestData = new GetReplicaLogInfoRequestData()
                    .setBrokerId(previousWork.brokerId);
            for (GetReplicaLogInfoResponseData.TopicPartitionLogInfo tp: responseData.topicPartitionLogInfoList()) {
                List<Integer> partitions = new ArrayList<>();
                for (GetReplicaLogInfoResponseData.PartitionLogInfo li: tp.partitionLogInfo()) {
                    if (li.errorCode() != Errors.NONE.code()) {
                        partitions.add(li.partition());
                    }
                }
                if (partitions.size() > 0) {
                    requestData.topicPartitions().add(new GetReplicaLogInfoRequestData.TopicPartitions()
                            .setTopicId(tp.topicId())
                            .setPartitions(partitions));
                }
            }
            var backoff = new ExponentialBackoff(
                    BACKOFF_INITIAL_INTERVAL_MS,
                    BACKOFF_MULTIPLIER_MS,
                    BACKOFF_MAX_MS,
                    BACKOFF_JITTER);
            var nextRetryCount = previousWork.retryCount + 1;
            var nextWork = new RecoveryLogRequestWork(
                    new GetReplicaLogInfoRequest.Builder(requestData),
                    previousWork.driver,
                    previousWork.machine,
                    previousWork.node,
                    previousWork.brokerId,
                    nextRetryCount
            );
            return new Result(nextWork, responseData, backoff.backoff(nextRetryCount), false);
        }
    }
}
