package org.apache.kafka.controller.recoverymanager;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.InterBrokerSendThread;
import org.apache.kafka.server.util.RequestAndCompletionHandler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;

public class UncleanRecoveryRequestThread extends InterBrokerSendThread {
    private final LinkedBlockingQueue<RecoveryLogRequestWork> queue;
    private final Time time;

    public UncleanRecoveryRequestThread(String name,
                                        KafkaClient networkClient,
                                        int requestTimeoutMs,
                                        Time time) {
        super(name, networkClient, requestTimeoutMs, time);
        this.queue = new LinkedBlockingQueue<>();
        this.time = time;
    }

    public void enqueueWork(RecoveryLogRequestWork work) {
        queue.add(work);
    }

    @Override
    public Collection<RequestAndCompletionHandler> generateRequests() {
        ArrayList<RequestAndCompletionHandler> requests = new ArrayList<>(this.queue.size());
        Iterator<RecoveryLogRequestWork> iterator = this.queue.iterator();
        while (iterator.hasNext()) {
            RecoveryLogRequestWork work = iterator.next();
            requests.add(new RequestAndCompletionHandler(time.milliseconds(), work.node, work.builder, work));
            iterator.remove();
        }
        return requests;
    }

}
