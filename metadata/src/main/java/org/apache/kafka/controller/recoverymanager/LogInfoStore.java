package org.apache.kafka.controller.recoverymanager;

import org.apache.kafka.server.common.TopicIdPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LogInfoStore {
    private final Map<TopicIdPartition, Map<Integer, EpochOffset>> store;

    public LogInfoStore() {
        this.store = new HashMap<>();
    }

    public void add(TopicIdPartition tp, int replica, EpochOffset eo) {
        if (store.containsKey(tp)) {
            store.get(tp).put(replica, eo);
        } else {
            Map<Integer, EpochOffset> map = new HashMap<>();
            map.put(replica, eo);
            store.put(tp, map);
        }
    }

    public Optional<EpochOffset> get(TopicIdPartition topicIdPartition, int replica) {
        if (store.containsKey(topicIdPartition)) {
            return Optional.of(store.get(topicIdPartition).get(replica));
        }
        return Optional.empty();
    }

    public static class EpochOffset implements Comparable<EpochOffset> {
        public final int epoch;
        public final int offset;

        EpochOffset(int epoch, int offset) {
            this.epoch = epoch;
            this.offset = offset;
        }

        public static final EpochOffset MIN = new EpochOffset(Integer.MIN_VALUE, Integer.MIN_VALUE);
        public static final EpochOffset MAX = new EpochOffset(Integer.MAX_VALUE, Integer.MAX_VALUE);

        @Override
        public int compareTo(EpochOffset o) {
            if (this.epoch == o.epoch) {
                return this.offset - o.offset;
            }
            return this.epoch - o.epoch;
        }
    }
}
