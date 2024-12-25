package com.aliyun.ha3engine.jdbc.sdk.client;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author yongxing.dyx
 * @date 2024/12/17
 */
public class CloudClientHolder {
    private CloudClient client;
    private LongAdder ref;

    public CloudClientHolder(CloudClient client) {
        this.client = client;
        ref = new LongAdder();
    }

    public CloudClient getClient() {
        return client;
    }

    public long refInc() {
        ref.increment();
        return ref.sum();
    }

    public long refDec() {
        ref.decrement();
        return ref.sum();
    }

    public long refCount() {
        return ref.sum();
    }

    public void close() throws IOException {

        if (client != null) {
            client = null;
        }
    }
}
