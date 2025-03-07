package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.common.requests.ApiError;

// TODO we have this class because we must distinguish between "Errors.NONE" and a finished election
// but even then, when do we need to know if an election is finsihed?
public class TopicPartitionElectionState {
    enum ElectionState {
        PENDING,
        GATHERING_LOG_INFO,
        CONTROLLER_WRITE,
        COMPLETED,
        FAILED;
    }

    private ElectionState electionState;
    // TODO create special error state for "we are still waiting on this election"
    private ApiError error = ApiError.NONE;

    public TopicPartitionElectionState() {
        // TODO why must we do this?
        this.electionState = ElectionState.PENDING;
    }

    public void finish() {
        this.electionState = ElectionState.COMPLETED;
    }

    public void fail(ApiError error) {
        this.electionState = ElectionState.FAILED;
        this.error = error;
    }
}
