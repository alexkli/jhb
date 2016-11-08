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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

/**
 * Main application class.
 */
public class Jhb {

    public static class Config {
        @Parameter(description = "url", required = true)
        public List<String> url;

        @Parameter(names = "-t", description = "number of concurrent request threads")
        public Integer threads = 50;

        @Parameter(names = "-d", description = "duration in seconds")
        public Integer duration = 10;

        @Parameter(names = "-r", description = "request rate per second (fixed)")
        public Integer rate = 10;

        @Parameter(names = "--client", description = "http client: java, ahc, fast, nio (default)")
        public String client = "nio";

//        @Parameter(names = "--help", help = true)
//        private boolean help;
//        @Parameter(names = {"-h", "-?", "--help"}, help = true, description = "show help")
//        private Object help;
//        @Parameter(names = "--help", arity = 0, help = true)
//        private Object help;
    }

    public static void main(String[] args) throws InterruptedException {
        Config config = parseCommandLine(args);

        final HttpGet request = new HttpGet(config.url.get(0));
        Date now = new Date();
        System.out.println("Time    : " + new SimpleDateFormat("HH:mm:ss").format(now));
        System.out.println("Duration:       " + config.duration + " seconds");
        Calendar cal = Calendar.getInstance();
        cal.setTime(now);
        cal.add(Calendar.SECOND, config.duration);
        System.out.println("Until   : " + new SimpleDateFormat("HH:mm:ss").format(cal.getTime()));
        System.out.println("Request : " + request);
        System.out.println("Threads : " + config.threads);
        System.out.println("Rate    : " + config.rate + " req/sec");
        System.out.println("Client  : " + config.client);

        // setup components
        BlockingQueue<QueueItem<HttpRequestBase>> queue = new LinkedBlockingQueue<>();

        CloseableHttpClient httpClient = createHttpClient(config.threads);
        Collection<Worker> workers = new ArrayList<>(config.threads);
        for (int i = 0; i < config.threads; i++) {
            Worker worker = new Worker(httpClient, queue, config);
            workers.add(worker);
            worker.start();
        }
        Producer producer = new Producer(queue, request, config);

        // start filling queue & measuring
        long start = System.currentTimeMillis();

        producer.start();
        producer.join();

        // end workers
        long finalLength = queue.size();
        queue.clear();
        for (Worker ignored : workers) {
            queue.add(new PoisonPill<>());
        }
        for (Worker worker : workers) {
            worker.join();
        }

        long durationSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - start);
        long finishedRequests = producer.getFinishedRequests();

        System.out.println();
        System.out.println("Total time        : " + durationSec + " seconds");
        System.out.println("Queued requests   : " + producer.getQueuedRequests());
        System.out.println("Finished requests : " + finishedRequests);
        System.out.println("Failed requests   : " + producer.getFailedRequests());
        System.out.println();
        System.out.println("Throughput avg    : " + finishedRequests/durationSec + " req/sec");
        System.out.println("Mean response time: " + formatHumanTime(producer.getResponseTimeAvg(), TimeUnit.MICROSECONDS));
        System.out.println("Mean service time : " + formatHumanTime(producer.getServiceTimeAvg(), TimeUnit.MICROSECONDS));
//        System.out.println("Mean response time: " + formatHumanTime(producer.getResponseTimeHistogram().getMean(), TimeUnit.MICROSECONDS));
//        System.out.println("Mean service time : " + formatHumanTime(producer.getServiceTimeHistogram().getMean(), TimeUnit.MICROSECONDS));
        System.out.println();
        System.out.println("Max queue length  : " + producer.getMaxLength());
        System.out.println("Outstanding length: " + finalLength);

        RunningAverage workerIdleAvg = new RunningAverage();
        for (Worker worker : workers) {
            workerIdleAvg.add(worker.getAverageIdleNanos());
        }
        System.out.println("Worker idle avg   : " + formatHumanTime(workerIdleAvg.getAverage(), TimeUnit.NANOSECONDS));
    }

    private static String formatHumanTime(double time, TimeUnit unit) {
        boolean done = false;
        while (!done) {
            switch (unit) {
                case NANOSECONDS:
                    if (time < 1000) {
                        done = true;
                    } else {
                        time /= 1000;
                        unit = TimeUnit.MICROSECONDS;
                    }
                    break;
                case MICROSECONDS:
                    if (time < 1000) {
                        done = true;
                    } else {
                        time /= 1000;
                        unit = TimeUnit.MILLISECONDS;
                    }
                    break;
                case MILLISECONDS:
                    if (time < 1000) {
                        done = true;
                    } else {
                        time /= 1000;
                        unit = TimeUnit.SECONDS;
                    }
                    break;
                case SECONDS:
                    if (time < 60) {
                        done = true;
                    } else {
                        time /= 60;
                        unit = TimeUnit.MINUTES;
                    }
                    break;
                case MINUTES:
                    if (time < 60) {
                        done = true;
                    } else {
                        time /= 60;
                        unit = TimeUnit.HOURS;
                    }
                    break;
                case HOURS:
                    if (time < 24) {
                        done = true;
                    } else {
                        time /= 24;
                        unit = TimeUnit.DAYS;
                    }
                    break;
            }
        }
        return String.format(Locale.ENGLISH, "%.2f", time) + " " + unit.name().toLowerCase();
    }

    private static CloseableHttpClient createHttpClient(int THREADS) {
        PoolingHttpClientConnectionManager connectionMgr = new PoolingHttpClientConnectionManager();
        connectionMgr.setMaxTotal(THREADS);
        // same URL = route for all requests, so maxPerRoute = maxTotal
        connectionMgr.setDefaultMaxPerRoute(THREADS);

        return HttpClients.custom()
            .setConnectionManager(connectionMgr)
            .build();
    }

    private static Config parseCommandLine(String... args) {
        Config config = new Config();
        JCommander jCommander = new JCommander(config);
        jCommander.setProgramName("jhb");

        try {
            jCommander.parse(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            jCommander.usage();
            System.exit(1);
        }

//        if (config.help) {
//            jCommander.usage();
//            System.exit(0);
//        }

        return config;
    }

/*
    private static void usage(JCommander jCommander) {
        PrintStream out = System.out;
        out.println("usage: jhb [options] url");
        for (ParameterDescription param : jCommander.getParameters()) {
            out.append("    ");
            out.append(param.getNames());
            out.append(" ").append(param.getDescription());
            out.println();
        }
    }
*/
}
