package github.luckygc.pgq.impl;

import github.luckygc.pgq.api.MessageQueue;
import github.luckygc.pgq.api.manager.MessageManager;
import github.luckygc.pgq.api.callback.MessageAvailableCallback;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessagesAvailableCallBackImpl implements MessageAvailableCallback {

    private static final Logger log = LoggerFactory.getLogger(MessagesAvailableCallBackImpl.class);

    @Override
    public void onMessageAvailable() {
        if (!semaphore.tryAcquire()) {
            return;
        }

        try {
            threadPool.execute(() -> {
                try {
                    processMessages();
                } finally {
                    semaphore.release();
                }
            });
        } catch (Throwable t) {
            semaphore.release();
            log.error("提交消息处理任务失败", t);
        }
    }
}
