package org.apache.kafka.controller.recoverymanager;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.GetReplicaLogInfoResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class ElectionRequestWorker extends InterBrokerSendThread {
    // TODO make this an explicitly sized queue
    //      in fact we want to do this to all of our queues. They should be array blocking queues with
    //      finite size for efficiency and correctness.
    // TODO also check if there is an internal data structure for this queue...
    private final LinkedBlockingQueue<Work> queue;

    public ElectionRequestWorker(String name,
                                 KafkaClient networkClient,
                                 int requestTimeoutMs,
                                 Time time) {
        super(name, networkClient, requestTimeoutMs, time);
        this.queue = new LinkedBlockingQueue<>();
    }

    public void enqueueWork(Work work) {
        queue.add(work);
    }

    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        // TODO we need to identify a way to cancel specific requests
        //      to cancel we need to do either:
        // 1. keep track of which requests are associated with with each ongoing election. This allows us to
        //    walk through our queue and remove requests which are not needed
        // 2. or keep a special value in RequestAndCompletionHandler which allows us
        ArrayList<RequestAndCompletionHandler> requests = new ArrayList<>(this.queue.size());
        Iterator<Work> iterator = this.queue.iterator();
        while (iterator.hasNext()) {
            Work work = iterator.next();
            requests.add(new RequestAndCompletionHandler(Time.SYSTEM.milliseconds(), work.node, work.builder, work));
            iterator.remove();
        }
        return requests;
    }

    // TODO add a retry count...
    // TODO error handling
    static class Work implements RequestCompletionHandler {
        public final AbstractRequest.Builder<?> builder;
        public final Node node;

        private final ElectionDriver driver;
        private final OngoingElectionStateMachine ongoing;

        public Work(AbstractRequest.Builder<?> builder, ElectionDriver driver, OngoingElectionStateMachine ongoing, Node node) {
            this.builder = builder;
            this.driver = driver;
            this.ongoing = ongoing;
            this.node = node;
        }

        @Override
        public void onComplete(ClientResponse response) {
            // TODO error handling...
            GetReplicaLogInfoResponse logInfoResponse = (GetReplicaLogInfoResponse) response.responseBody();
            GetReplicaLogInfoResponseData responseData = logInfoResponse.data();
            driver.addReplicaLogResponse(responseData, ongoing, node);
        }
    }

}
