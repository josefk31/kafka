package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ElectLeadersRequestData;
import org.apache.kafka.common.message.GetReplicaLogInfoRequestData;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetReplicaLogInfoRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.controller.ReplicationControlManager;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.common.TopicIdPartition;

import java.util.*;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

interface ElectionsManager {
    Future<ApiMessage> handleLeadershipElection(ElectLeadersRequestData requestData, Long deadline);
}

// ElectionRequest -> TopicPartiton -> Replica -> Log

class BuilderBuilderLol {
    private Map<Uuid, List<Integer>> partitionForTopic;

    public BuilderBuilderLol() {
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

// Defines a state machine to handle all elections; what happens with the result?
// we put the result somewhere...
// rename to ongoing election or something...
class OngoingElectionRequest {
    // Multiple dimensions will point to a single state machine
    // A node and log information about a topic partition on that node corresponds to each state machine
    // But an ElectionStateMachine is an election for a specific TopicPartition...
    private Map<ElectionStateMachine.Dimension, ElectionStateMachine> electionStateMachines;
    private final CompletableFuture<ApiMessage> future;
    private final long deadline;

    OngoingElectionRequest(Map<ElectionStateMachine.Dimension, ElectionStateMachine> electionStateMachines, long deadline) {
        this.electionStateMachines = electionStateMachines;
        this.future = new CompletableFuture<ApiMessage>();
        this.deadline = deadline;
    }

    public void updateElectionStates(Node node, GetReplicaLogInfoResponseData responseData) {
        for (GetReplicaLogInfoResponseData.TopicPartitionLogInfo tp: responseData.topicPartitionLogInfoList()) {
            // Handle empty...
            // TODO Hold on; what if this changes while we are running?
            for (GetReplicaLogInfoResponseData.PartitionLogInfo logInfo: tp.partitionLogInfo()) {
                TopicIdPartition tip = new TopicIdPartition(tp.topicId(), logInfo.partition());
                ElectionStateMachine.Dimension dimension = new ElectionStateMachine.Dimension(node, tip);
                electionStateMachines.get(dimension).addResult(new ElectionStateMachine.LogResult(tip, node.id(), logInfo));
            }
        }
    }

    public List<ElectionStateMachine> stateMachinesWithState(ElectionState electionState) {
        return electionStateMachines.values()
                .stream()
                .filter(machine -> machine.electionState() == electionState)
                .toList();
    }

    public Future<ApiMessage> requestFuture() {
        return future;
    }

    public void completeRequest() {
        this.future.complete(null);
    }

    public long deadline() {
        return deadline;
    }

    static class Builder {
        private List<ElectionStateMachine.Dimension> dimensions;
        // TODO metadata cache will 100% change while we have ongoing elections
        private long deadline;

        public Builder(long deadline) {
            // TODO figure out init capacity
            this.dimensions = new ArrayList<>();
            this.deadline = deadline;
        }

        public void scheduleElection(TopicIdPartition tp, Node node) {
            ElectionStateMachine.Dimension dimension = new ElectionStateMachine.Dimension(node, tp);
            dimensions.add(dimension);
        }

        public OngoingElectionRequest build() {
            Map<TopicIdPartition, ElectionStateMachine> stateMachineMap = new HashMap<>();
            Map<ElectionStateMachine.Dimension, ElectionStateMachine> dimensionMap = new HashMap<>();
            for (var dimension: dimensions) {
                ElectionStateMachine machine = stateMachineMap.get(dimension);
                if (machine == null) {
                    machine = new ElectionStateMachine();
                    stateMachineMap.put(dimension.topicPartition, machine);
                }
                dimensionMap.put(dimension, machine);
            }
            return new OngoingElectionRequest(dimensionMap, deadline);
        }
    }
}

public class ElectionDriver implements AutoCloseable {
    private ElectionRequestWorker electionRequestWorker;
    private List<OngoingElectionRequest> electionRequests;
    private KafkaEventQueue queue;
    // TODO will this ever change?
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
        final public OngoingElectionRequest ongoing;
        final public Node node;

        public ReplicaLogRequestDone(GetReplicaLogInfoResponseData responseData, OngoingElectionRequest ongoing, Node node) {
            this.responseData = responseData;
            this.ongoing = ongoing;
            this.node = node;
        }

        @Override
        public void run() throws Exception {
            ongoing.updateElectionStates(node, responseData);
            List<ElectionStateMachine> readStateMachines = ongoing.stateMachinesWithState(ElectionState.READY_FOR_ELECTION);
            for (ElectionStateMachine machine: readStateMachines) {

            }
        }
    }

    void appendReplicaLogResponse(GetReplicaLogInfoResponseData responseData, OngoingElectionRequest ongoingElectionRequest, Node node) {
        this.queue.append(new ReplicaLogRequestDone(responseData, ongoingElectionRequest, node));

    }

    class PeriodicEvent implements EventQueue.Event {
        @Override
        public void run() throws Exception {
            // 1. remove ongoing elections which are "completed"
            Iterator<OngoingElectionRequest> electionRequestsIterator = electionRequests.iterator();
            // TODO check whether this is wall clock or system time; prefer wall clock
            long now = time.hiResClockMs();
            while (electionRequestsIterator.hasNext()) {
                OngoingElectionRequest ongoing = electionRequestsIterator.next();
                if (ongoing.deadline() < now) {
                    // TODO figure out how to cancel outgoing requests if this case occurs
                    electionRequestsIterator.remove();
                }
            }
        }
    }

    class BeginElection implements EventQueue.Event {
        private final List<TopicElectionInstruction> topicsAndReplicas;
        private final List<BrokerRegistration> brokers;
        private final long deadline;
        private final ElectionDriver driver;

        public BeginElection(List<TopicElectionInstruction> topicsAndReplicas, List<BrokerRegistration> brokers, long deadline, ElectionDriver driver) {
            this.topicsAndReplicas = topicsAndReplicas;
            this.brokers = brokers;
            this.deadline = deadline;
            this.driver = driver;
        }

        @Override
        public void run() throws Exception {
            // TODO change deadline to long
            long currentTime = time.hiResClockMs();
            OngoingElectionRequest.Builder builder = new OngoingElectionRequest.Builder(currentTime + deadline);
            Map<Node, BuilderBuilderLol> requestBuilders = new HashMap<>();
            Map<Integer, BrokerRegistration> brokerRegistrations = new HashMap<>();
            for (BrokerRegistration registration: brokers) {
                brokerRegistrations.put(registration.id(), registration);
            }
            for (TopicElectionInstruction tr: topicsAndReplicas) {
                // TODO listener name adjustment
                tr.replicas.forEach(replica -> {
                    BrokerRegistration reg = brokerRegistrations.get(replica);
                    // TODO figure out how to get the listener configuration in the right place
                    Node node = reg.node("").get();
                    builder.scheduleElection(tr.topicIdPartition, node);
                    // TODO find simpler way
                    var rb = requestBuilders.putIfAbsent(node, new BuilderBuilderLol());
                    if (rb == null) {
                        rb = requestBuilders.get(node);
                    }
                    rb.addTopic(node, tr.topicIdPartition);
                });
            }

            OngoingElectionRequest ongoingElectionRequest = builder.build();
            requestBuilders.entrySet().forEach(entry -> {
                AbstractRequest.Builder<?> requestBuilder = new GetReplicaLogInfoRequest.Builder(entry.getValue().build().setBrokerId(entry.getKey().id()));
                ElectionRequestWork work = new ElectionRequestWork(requestBuilder, driver, ongoingElectionRequest, entry.getKey());
                electionRequestWorker.enqueueWork(work);
            });
        }
    }

    static class TopicElectionInstruction {
        final TopicIdPartition topicIdPartition;
        final List<Integer> replicas;

        TopicElectionInstruction(TopicIdPartition topicIdPartition, List<Integer> replicas) {
            this.topicIdPartition = topicIdPartition;
            this.replicas = replicas;
        }
    }

    public void startLeadershipElection(List<TopicElectionInstruction> topicsAndReplicas, List<BrokerRegistration> brokers, long deadline) {
        queue.append(new BeginElection(topicsAndReplicas, brokers, deadline,this));
    }

    class PartitionWritten implements EventQueue.Event {
        @Override
        public void run() throws Exception {
        }
    }


    @Override
    public void close() throws Exception {
        this.queue.close();
    }
}