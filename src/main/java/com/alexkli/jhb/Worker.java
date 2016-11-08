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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketOption;
import java.net.SocketOptions;
import java.net.StandardSocketOptions;
import java.net.URI;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;

/**
 * Workers take requests from the queue and run them over http.
 */
class Worker extends Thread {

    private final CloseableHttpClient httpClient;
    private final HttpClientContext context;
    private final BlockingQueue<QueueItem<HttpRequestBase>> queue;
    private final RunningAverage idleAvg = new RunningAverage();
    private final Jhb.Config config;

    public Worker(CloseableHttpClient httpClient, BlockingQueue<QueueItem<HttpRequestBase>> queue, Jhb.Config config) {
        this.httpClient = httpClient;
        this.context = HttpClientContext.create();
        this.queue = queue;
        this.config = config;
    }

    @Override
    public void run() {
        try {
            while (true) {
                long start = System.nanoTime();

                QueueItem<HttpRequestBase> item = queue.take();

                idleAvg.add(System.nanoTime() - start);

                if (item.isPoisonPill()) {
                    return;
                }

                HttpRequestBase request = item.getRequest();

                if ("java".equals(config.client)) {
                    System.setProperty("http.keepAlive", "false");

                    item.sent();

                    try {
                        HttpURLConnection http = (HttpURLConnection) new URL(request.getURI().toString()).openConnection();
                        http.setConnectTimeout(5000);
                        http.setReadTimeout(5000);
                        int statusCode = http.getResponseCode();

                        consumeAndCloseStream(http.getInputStream());

                        if (statusCode == 200) {
                            item.done();
                        } else {
                            item.failed();
                        }
                    } catch (IOException e) {
                        System.err.println("Failed request: " + e.getMessage());
                        e.printStackTrace();
//                        System.exit(2);
                        item.failed();
                    }
                } else if ("ahc".equals(config.client)) {
                    try {
                        item.sent();

                        try (CloseableHttpResponse response = httpClient.execute(request, context)) {
                            int statusCode = response.getStatusLine().getStatusCode();
                            if (statusCode == 200) {
                                item.done();
                            } else {
                                item.failed();
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("Failed request: " + e.getMessage());
                        item.failed();
                    }
                } else if ("fast".equals(config.client)) {
                    try {
                        URI uri = request.getURI();

                        item.sent();

                        InetAddress addr = InetAddress.getByName(uri.getHost());
                        Socket socket = new Socket(addr, uri.getPort());
                        PrintWriter out = new PrintWriter(socket.getOutputStream());
//                        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                        // send an HTTP request to the web server
                        out.println("GET / HTTP/1.1");
                        out.append ("Host: ").append(uri.getHost()).append(":").println(uri.getPort());
                        out.println("Connection: Close");
                        out.println();
                        out.flush();

                        // read the response
                        consumeAndCloseStream(socket.getInputStream());
//                        boolean loop = true;
//                        StringBuilder sb = new StringBuilder(8096);
//                        while (loop) {
//                            if (in.ready()) {
//                                int i = 0;
//                                while (i != -1) {
//                                    i = in.read();
//                                    sb.append((char) i);
//                                }
//                                loop = false;
//                            }
//                        }
                        item.done();
                        socket.close();

                    } catch (IOException e) {
                        e.printStackTrace();
                        item.failed();
                    }
                } else if ("nio".equals(config.client)) {
                    URI uri = request.getURI();

                    item.sent();

                    String requestBody =
                        "GET / HTTP/1.1\n" +
                        "Host: " + uri.getHost() + ":" + uri.getPort() + "\n" +
                        "Connection: Close\n\n";

                    try {
                        InetSocketAddress addr = new InetSocketAddress(uri.getHost(), uri.getPort());
                        SocketChannel channel = SocketChannel.open();
                        channel.socket().setSoTimeout(5000);
                        channel.connect(addr);

                        ByteBuffer msg = ByteBuffer.wrap(requestBody.getBytes());
                        channel.write(msg);
                        msg.clear();

                        ByteBuffer buf = ByteBuffer.allocate(1024);

                        int count;
                        while ((count = channel.read(buf)) != -1) {
                            buf.flip();

                            byte[] bytes = new byte[count];
                            buf.get(bytes);

                            buf.clear();
                        }
                        channel.close();

                        item.done();

                    } catch (IOException e) {
                        e.printStackTrace();
                        item.failed();
                    }
                }
            }
        } catch (InterruptedException e) {
            System.err.println("Worker thread [" + this.toString() + "] was interrupted: " + e.getMessage());
        }
    }

    private void consumeAndCloseStream(InputStream responseStream) throws IOException {
        try (InputStream ignored = responseStream) {
            IOUtils.copyLarge(responseStream, new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    // discard
                }

                @Override
                public void write(byte[] b) throws IOException {
                    // discard
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    // discard
                }
            });
        }
    }

    public double getAverageIdleNanos() {
        return idleAvg.getAverage();
    }
}
