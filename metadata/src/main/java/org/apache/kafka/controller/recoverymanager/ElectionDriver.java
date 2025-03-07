package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.GetReplicaLogInfoRequestData;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetReplicaLogInfoRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.common.TopicIdPartition;

// TODO don't use wildcard import...
import java.util.*;
import java.util.List;
import java.util.concurrent.Future;

interface ElectionsManager {
    Future<ApiMessage> handleLeadershipElection(ElectLeadersRequestData requestData, Long deadline);
}

// ElectionRequest -> TopicPartiton -> Replica -> Log
// TODO potentially make this object at a higher level
class RequestAmoritizer {
    private Map<Uuid, List<Integer>> partitionForTopic;

    public RequestAmoritizer() {
        partitionForTopic = new HashMap<>();
    }

    public void addTopic(Node node, TopicIdPartition tp) {
        partitionForTopic.putIfAbsent(tp.topicId(), Collections.emptyList());
        partitionForTopic.get(tp.topicId()).add(tp.partitionId());

    }

    public GetReplicaLogInfoRequestData build() {
        GetReplicaLogInfoRequestData requestData = new GetReplicaLogInfoRequestData();
        // TODO why broker ID again??
        for (Map.Entry<Uuid, List<Integer>> entry : partitionForTopic.entrySet()) {
            var topicPartitions = new GetReplicaLogInfoRequestData.TopicPartitions();
            requestData.topicPartitions().add(new GetReplicaLogInfoRequestData.TopicPartitions()
                    .setTopicId(entry.getKey())
                    .setPartitions(entry.getValue()));
        }
        return requestData;
    }
}

public class ElectionDriver implements AutoCloseable {
    private ElectionRequestWorker electionRequestWorker;
    private List<OngoingElectionStateMachine> electionRequests;
    private KafkaEventQueue queue;
    private Time time;
    private Controller controller;

    public ElectionDriver(String name, ElectionRequestWorker electionRequestWorker, Controller controller) {
        this.electionRequestWorker = electionRequestWorker;
        this.electionRequests = new LinkedList<>();
        this.controller = controller;
    }

    // TODO are there other types of elections?
    //      can someone cancel an ongoing election?
    class ReplicaLogRequestDone implements EventQueue.Event {
        final public GetReplicaLogInfoResponseData responseData;
        final public OngoingElectionStateMachine ongoing;
        final public Node node;

        public ReplicaLogRequestDone(GetReplicaLogInfoResponseData responseData, OngoingElectionStateMachine ongoing, Node node) {
            this.responseData = responseData;
            this.ongoing = ongoing;
            this.node = node;
        }

        @Override
        public void run() throws Exception {
            // TODO update election states when a response is received
            ongoing.updateElectionStates(node, responseData);
        }
    }

    void addReplicaLogResponse(GetReplicaLogInfoResponseData responseData, OngoingElectionStateMachine ongoingElectionStateMachine, Node node) {
        this.queue.append(new ReplicaLogRequestDone(responseData, ongoingElectionStateMachine, node));

    }

    class PeriodicEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            // 1. remove ongoing elections which are "completed"
            Iterator<OngoingElectionStateMachine> electionRequestsIterator = electionRequests.iterator();
            // TODO check whether this is wall clock or system time; prefer wall clock
            long now = time.hiResClockMs();
            while (electionRequestsIterator.hasNext()) {
                OngoingElectionStateMachine ongoing = electionRequestsIterator.next();
                if (ongoing.gatherDeadline() < now) {
                    // TODO figure out how to cancel outgoing requests if this case occurs
                    electionRequestsIterator.remove();
                    controller
                            .performUncleanRecovery(ongoing.topics().stream().toList(), ongoing.store())
                            .handle((results, error) -> {
                                // TODO error handling...
                                // at this point we may wish to retry or we may wish to just cancel
                                // the entire election...
                                queue.append(new PartitionWritten(ongoing, results));
                                return results;
                            });
                }
            }
        }
    }

    class BeginElection implements EventQueue.Event {
        private final List<TopicElectionInstruction> topicsAndReplicas;
        private final Map<Integer, BrokerRegistration> brokerRegistrations;
        private final long deadline;
        private final ElectionDriver driver;

        public BeginElection(List<TopicElectionInstruction> topicsAndReplicas, Map<Integer, BrokerRegistration> brokerRegistrations, long deadline, ElectionDriver driver) {
            this.topicsAndReplicas = topicsAndReplicas;
            this.brokerRegistrations = brokerRegistrations;
            this.deadline = deadline;
            this.driver = driver;
        }

        @Override
        public void run() throws Exception {
            List<TopicIdPartition> topicIdPartitions = new ArrayList<>();
            Map<Node, RequestAmoritizer> requestBuilders = new HashMap<>();
            for (TopicElectionInstruction tr: topicsAndReplicas) {
                for (int replica: tr.replicas) {
                    BrokerRegistration reg = brokerRegistrations.get(replica);
                    // TODO figure out how to get the listener configuration in the right place
                    Node node = reg.node("").get();
                    RequestAmoritizer rb = null;
                    if (requestBuilders.containsKey(node)) {
                        rb = requestBuilders.get(node);
                    } else {
                        rb = new RequestAmoritizer();
                        requestBuilders.put(node, rb);
                    }
                    rb.addTopic(node, tr.topicIdPartition);
                }
            }

            long gatherDeadline = time.hiResClockMs() + deadline;
            OngoingElectionStateMachine ongoingElectionStateMachine = new OngoingElectionStateMachine(topicIdPartitions, gatherDeadline);
            requestBuilders.entrySet().forEach(entry -> {
                Node node = entry.getKey();
                GetReplicaLogInfoRequestData requestData = entry.getValue().build().setBrokerId(node.id());
                AbstractRequest.Builder<?> requestBuilder = new GetReplicaLogInfoRequest.Builder(requestData);
                ElectionRequestWork work = new ElectionRequestWork(requestBuilder, driver, ongoingElectionStateMachine, entry.getKey());
                electionRequestWorker.enqueueWork(work);
            });
        }
    }

    public static class TopicElectionInstruction {
        final TopicIdPartition topicIdPartition;
        final int[] replicas;

        public TopicElectionInstruction(TopicIdPartition topicIdPartition, int[] replicas) {
            this.topicIdPartition = topicIdPartition;
            this.replicas = replicas;
        }
    }

    public void startLeadershipElection(List<TopicElectionInstruction> topicsAndReplicas,
                                        Map<Integer, BrokerRegistration> brokerRegistration,
                                        long deadline) {
        queue.append(new BeginElection(topicsAndReplicas, brokerRegistration, deadline,this));
    }

    class PartitionWritten implements EventQueue.Event {
        private final OngoingElectionStateMachine ongoing;
        private final List<UncleanRecoveryResult> results;

        PartitionWritten(OngoingElectionStateMachine ongoing, List<UncleanRecoveryResult> results) {
            this.ongoing = ongoing;
            this.results = results;
        }

        @Override
        public void run() throws Exception {
            results.forEach(ongoing::completeElection);
        }
    }

    @Override
    public void close() throws Exception {
        this.queue.close();
    }
}

