/**************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *************************************************************************/

package com.alexkli.jhb;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.HdrHistogram.Histogram;
import org.apache.http.client.methods.HttpRequestBase;

/**
 * Adds requests to the queue and measures the throughput.
 */
public class Producer extends Thread {

    private final BlockingQueue<QueueItem<HttpRequestBase>> queue;
    private final HttpRequestBase request;
    private final Jhb.Config config;
    private long queuedRequests;
    private long maxLength;
    private AtomicLong finishedRequests = new AtomicLong();
    private AtomicLong failedRequests = new AtomicLong();

    /** micro seconds, 1 hour max */
//    private Histogram responseTimeHistogram = new Histogram(3600000000L, 3);
    private Histogram responseTimeHistogram = new Histogram(1);
    private RunningAverage responseTimeAvg = new RunningAverage();

    /** micro seconds, 1 hour max */
//    private Histogram serviceTimeHistogram = new Histogram(3600000000L, 3);
    private Histogram serviceTimeHistogram = new Histogram(1);
    private RunningAverage serviceTimeAvg = new RunningAverage();

    public Producer(BlockingQueue<QueueItem<HttpRequestBase>> queue, HttpRequestBase request, Jhb.Config config) {
        this.queue = queue;
        this.request = request;
        this.config = config;
    }

    @Override
    public void run() {
        int runtimeMillis = config.duration * 1000;
        long start = System.currentTimeMillis();

        long requestsPerRun = config.threads;

        long intervalNanos = requestsPerRun * TimeUnit.SECONDS.toNanos(1) / config.rate;

        while (true) {
            nanoSleep(intervalNanos);

            long length = queue.size();
            if (length > maxLength) {
                maxLength = length;
            }
//            if (length > 10) {
//                System.out.println("[" + this.toString() + "] queue length: " + length);
//            }

            for (int i = 0; i < requestsPerRun; i++) {
                queuedRequests++;

                if (!queue.offer( new HttpRequestQueueItem(clone(request)) )) {
                    System.out.println("Queue too big");
                }

                if (queuedRequests % 1000 == 0) {
                    System.out.println(queuedRequests + " requests queued so far");
                }
            }

            if (System.currentTimeMillis() - start >= runtimeMillis) {
                break;
            }
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private static void nanoSleep(long nanoseconds) {
        long start = System.nanoTime();
        while(System.nanoTime() - start < nanoseconds);
    }

    private static HttpRequestBase clone(HttpRequestBase obj) {
        try {
            return (HttpRequestBase) obj.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    public long getQueuedRequests() {
        return queuedRequests;
    }

    public long getFinishedRequests() {
        return finishedRequests.get();
    }

    public long getFailedRequests() {
        return failedRequests.get();
    }

    public long getMaxLength() {
        return maxLength;
    }

    public Histogram getResponseTimeHistogram() {
        return responseTimeHistogram;
    }

    public Histogram getServiceTimeHistogram() {
        return serviceTimeHistogram;
    }

    public double getResponseTimeAvg() {
        return responseTimeAvg.getAverage();
    }

    public double getServiceTimeAvg() {
        return serviceTimeAvg.getAverage();
    }

    private class HttpRequestQueueItem implements QueueItem<HttpRequestBase> {
        private final HttpRequestBase request;
        private long queued;
        private long sent;

        public HttpRequestQueueItem(HttpRequestBase request) {
            this.request = request;
            this.queued = System.nanoTime();
        }

        @Override
        public HttpRequestBase getRequest() {
            return request;
        }

        @Override
        public void sent() {
            sent = System.nanoTime();
        }

        @Override
        public void done() {
            long end = System.nanoTime();
            long responseTime = end - queued;
            long serviceTime = end - sent;
            finishedRequests.incrementAndGet();
            if (queued > 0) {
//                    responseTimeHistogram.recordValue(TimeUnit.NANOSECONDS.toMicros(responseTime));
                responseTimeAvg.add(TimeUnit.NANOSECONDS.toMicros(responseTime));
            }
            if (sent > 0) {
//                    serviceTimeHistogram.recordValue(TimeUnit.NANOSECONDS.toMicros(serviceTime));
                serviceTimeAvg.add(TimeUnit.NANOSECONDS.toMicros(serviceTime));
            }
        }

        @Override
        public void failed() {
            failedRequests.incrementAndGet();
        }
    }
}
