package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.controller.Controller;
import org.apache.kafka.metadata.BrokerRegistration;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.common.TopicIdPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ElectionDriver implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(ElectionDriver.class);
    private final UncleanRecoveryRequestThread uncleanRecoveryRequestThread;
    private final KafkaEventQueue queue;
    private final Time time;
    private final Controller controller;
    private final LogContext logContext;
    private int electionId;

    public ElectionDriver(int nodeId,
                          UncleanRecoveryRequestThread uncleanRecoveryRequestThread,
                          Controller controller,
                          Time time) {
        this.uncleanRecoveryRequestThread = uncleanRecoveryRequestThread;
        this.time = time;
        this.logContext = new LogContext(String.format("[ElectionDriver id=%d] ", nodeId));
        this.queue = new KafkaEventQueue(time, logContext, String.format("election-driver-%d ", nodeId));
        this.controller = controller;
    }

    public static class Builder {
        private Controller controller;
        private Time time;
        private int nodeId;
        private KafkaClient kafkaClient;

        public Builder setKafkaClient(KafkaClient kafkaClient) {
            this.kafkaClient = kafkaClient;
            return this;
        }

        public Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setController(Controller controller) {
            this.controller = controller;
            return this;
        }

        public ElectionDriver build() {
            String electionWorkerName = String.format("election-worker-%d", nodeId);
            UncleanRecoveryRequestThread worker = new UncleanRecoveryRequestThread(electionWorkerName, kafkaClient,1000, this.time);
            return new ElectionDriver(this.nodeId, worker, this.controller, this.time);
        }
    }

    void processReplicaLogResponse(ElectionStateMachine machine, int brokerId, RecoveryLogRequestWork.Result result) {
        // A request could be completed after gathering has been completed;
        // in which case no further work is needed...
        if (machine.completedGathering()) {
            return;
        }
        // May not work; we should explicitly check for errors
        if (result.succeeded) {
            this.queue.append(() -> {
                // we will always pass through non-null response data
                assert result.responseData != null;
                machine.updateElectionStates(brokerId, result.responseData);
                machine.finishedRequestsForBroker();
                if (machine.completedGathering()) {
                    controller
                            .performUncleanRecovery(machine.topics().stream().toList(), machine.store())
                            .handle((results, error) -> {
                                queue.append(new PartitionWritten(machine, results));
                                return results;
                            });
                }
            });
        } else {
            if (result.responseData != null) {
                this.queue.append(() -> {
                    machine.updateElectionStates(brokerId, result.responseData);
                });
            }
            this.queue.scheduleDeferred(
                    String.format("retry-broker=%d-request-%d", brokerId, electionId),
                    new EventQueue.DeadlineFunction(result.backoffMs),
                    () -> uncleanRecoveryRequestThread.enqueueWork(result.nextRequest));
        }
    }

    private class SwitchToControllerElectionEvent implements EventQueue.Event {
        private final ElectionStateMachine machine;

        SwitchToControllerElectionEvent(ElectionStateMachine machine) {
            this.machine = machine;
        }

        @Override
        public void run() throws Exception {
            machine.stopGathering();
            if (machine.hasCompletedAllElections()) {

            }
            controller
                    .performUncleanRecovery(machine.topics().stream().toList(), machine.store())
                    .handle((results, error) -> {
                        if (error != null) {
                            log.warn("Unclean recovery resulted in error", error);
                        }
                        queue.append(new PartitionWritten(machine, results));
                        return results;
                    });
        }
    }

    private class BeginElection implements EventQueue.Event {
        private final List<TopicElectionInstruction> topicsAndReplicas;
        private final Map<Integer, BrokerRegistration> brokerRegistrations;
        private final long deadlineMs;
        private final ElectionDriver driver;

        public BeginElection(List<TopicElectionInstruction> topicsAndReplicas,
                             Map<Integer, BrokerRegistration> brokerRegistrations,
                             long deadlineMs,
                             ElectionDriver driver) {
            this.topicsAndReplicas = topicsAndReplicas;
            this.brokerRegistrations = brokerRegistrations;
            this.deadlineMs = deadlineMs;
            this.driver = driver;
        }

        @Override
        public void run() throws Exception {
            List<TopicIdPartition> topicIdPartitions = new ArrayList<>();
            LogRequestsAmortizer amoritizer = new LogRequestsAmortizer();
            for (TopicElectionInstruction tr: topicsAndReplicas) {
                for (int replica: tr.replicas) {
                    BrokerRegistration reg = brokerRegistrations.get(replica);
                    if (reg.fenced()) {
                        continue;
                    }
                    // Should be impossible
                    assert !reg.listeners().isEmpty();
                    String listenerName = reg.listeners().keySet().iterator().next();
                    Node node = reg.node(listenerName).get();
                    amoritizer.setNode(replica, node);
                    amoritizer.addTopic(replica, tr.topicIdPartition);
                }
            }
            long switchPointTimeMs = time.hiResClockMs() + deadlineMs;
            ElectionStateMachine newMachine = new ElectionStateMachine(topicIdPartitions);
            List<RecoveryLogRequestWork> requests = amoritizer.buildAll(driver, newMachine);
            newMachine.setBrokerGatheringCount(requests.size());
            requests.forEach(uncleanRecoveryRequestThread::enqueueWork);
            queue.scheduleDeferred(
                    String.format("periodic-event-%d", electionId),
                    new EventQueue.EarliestDeadlineFunction(TimeUnit.MILLISECONDS.toNanos(switchPointTimeMs)),
                    new SwitchToControllerElectionEvent(newMachine));
            electionId += 1;
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
                                        Map<Integer, BrokerRegistration> brokerRegistrations,
                                        long deadlineToStopGatheringMs) {
        queue.append(new BeginElection(topicsAndReplicas, brokerRegistrations, deadlineToStopGatheringMs,this));
    }

    private class PartitionWritten implements EventQueue.Event {
        private final ElectionStateMachine machine;
        private final List<UncleanRecoveryResult> results;

        PartitionWritten(ElectionStateMachine machine, List<UncleanRecoveryResult> results) {
            this.machine = machine;
            this.results = results;
        }

        @Override
        public void run() throws Exception {
            machine.completeElection(results);
        }
    }

    @Override
    public void close() throws Exception {
        this.queue.close();
    }
}

