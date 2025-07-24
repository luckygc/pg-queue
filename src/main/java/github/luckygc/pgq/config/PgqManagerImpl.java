package github.luckygc.pgq.config;

import github.luckygc.pgq.api.PgQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PgqManagerImpl {

    public final Map<String, PgQueue> queueMap = new ConcurrentHashMap<>();

    public void register(PgQueue queue) {
        queueMap.putIfAbsent(queue.getTopic(), queue);
    }
}
