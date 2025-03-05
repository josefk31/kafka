package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.message.GetReplicaLogInfoResponseData;
import org.apache.kafka.common.requests.GetReplicaLogInfoResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElectionRequestCompletionHandler implements RequestCompletionHandler {
    private static final Logger log = LoggerFactory.getLogger(ElectionRequestCompletionHandler.class);

    @Override
    public void onComplete(ClientResponse clientResponse) {
        GetReplicaLogInfoResponse response = (GetReplicaLogInfoResponse) clientResponse.responseBody();
        for (GetReplicaLogInfoResponseData.TopicPartitionLogInfo tp : response.data().topicPartitionLogInfoList()) {
            log.info("Node {} responded with: {}", clientResponse.destination(), tp);
        }
    }
}
