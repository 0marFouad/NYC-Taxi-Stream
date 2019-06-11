/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.neovisionaries.ws.client.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A source function that reads strings from a socket. The source will read bytes from the socket
 * stream and convert them to characters, each byte individually. When the delimiter character is
 * received, the function will output the current string, and begin a new string.
 *
 * <p>The function strips trailing <i>carriage return</i> characters (\r) when the delimiter is the
 * newline character (\n).
 *
 * <p>The function can be set to reconnect to the server socket in case that the stream is closed on
 * the server side.
 */
@PublicEvolving
public class SocketSource implements SourceFunction<Trip> {

    private static final long serialVersionUID = 1L;
    private final String hostname;
    private final String origin;
    private final String delimiter;

    private transient WebSocket currentWebSocket;

    private volatile boolean isRunning = true;

    public static volatile int recievedCounter = 0;

    public SocketSource(String hostname, String origin, String delimiter) {
        this.hostname = checkNotNull(hostname, "hostname must not be null");
        this.origin = origin;
        this.delimiter = delimiter;
    }

    @Override
    public void run(SourceContext<Trip> ctx) throws Exception {
        StringBuilder buffer = new StringBuilder();

        long attempt = 0;

        WebSocket websocket;
        if (origin != null) {
            websocket = new WebSocketFactory().createSocket(hostname).addHeader("Origin", origin);
        } else {
            websocket = new WebSocketFactory().createSocket(hostname);
        }

        this.currentWebSocket = websocket;

        websocket.addListener(new WebSocketAdapter() {
                    @Override
                    public void onTextMessage(WebSocket ws, String message) {
             buffer.append(message);
             recievedCounter++;
                    }
        }).connect();

        long oldTime = System.currentTimeMillis();
        while (isRunning) {
            int delimPos;
            while (buffer.length() >= 1 && (delimPos = buffer.indexOf(delimiter)) != -1) {
                String record = buffer.substring(0, delimPos + 1);
                Trip parsedRecord = new ObjectMapper().readValue(record, Trip.class);
                ctx.collect(parsedRecord);
                buffer.delete(0, delimPos + delimiter.length());
                oldTime = System.currentTimeMillis();
            }
            if ((System.currentTimeMillis() - oldTime) > 2000) {
                System.out.println("SOCKET STOP");
                websocket.disconnect();
                break;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        WebSocket theSocket = this.currentWebSocket;
        if (theSocket != null) {
            theSocket.disconnect();
        }
    }
}