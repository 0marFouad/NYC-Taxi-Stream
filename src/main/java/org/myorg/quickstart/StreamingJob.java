/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.myorg.quickstart;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Implements a streaming windowed version of the "WordCount" program.
 *
 * <p>This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text server (at port 12345)
 * using the <i>netcat</i> tool via
 * <pre>
 * nc -l 12345
 * </pre>
 * and run this example with the hostname and the port as arguments.
 */
@SuppressWarnings("serial")
public class StreamingJob {


	public static void main(String[] args) throws Exception {

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.enableCheckpointing(5000);
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

		// get input data by connecting to the socket
		SocketSource customSource = new SocketSource("ws://localhost:9000/ws", "http://localhost:9000", "}");
        DataStream<Trip> inputStream = env.addSource(customSource, (String)"Socket Stream");

		// parse the data, group it, window it, and aggregate the counts
//		DataStream<Trip> trips = inputStream
//				.map((line) -> (new ObjectMapper()).readValue(line, Trip.class));

		DataStream<ModifiedTrip> tripDataStream = inputStream
				.map(new MapTrip());




		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));

// use a ElasticsearchSink.Builder to create an ElasticsearchSink
		ElasticsearchSink.Builder<ModifiedTrip> esSinkBuilder = new ElasticsearchSink.Builder<>(
				httpHosts,
				new ElasticsearchSinkFunction<ModifiedTrip>() {
					public IndexRequest createIndexRequest(ModifiedTrip element) {
						Map<String, Object> json = new HashMap<>();
						json.put("day", element.getDay());
						json.put("taxiType", element.getTaxiType());

						json.put("vendorId", element.getVendorId());
						json.put("dropExist", element.getDropExist());

						json.put("BrooklynYellow", element.getBrooklynYellow());
						json.put("BrooklynGreen", element.getBrooklynGreen());
						json.put("BrooklynFHV", element.getBrooklynFHV());
						json.put("isBrooklyn", element.getIsBrooklyn());

						json.put("minutes", element.getMinutes());

						return Requests.indexRequest()
								.index("dashboard")
								.type("json")
								.source(json);
					}

					@Override
					public void process(ModifiedTrip element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				}
		);

		esSinkBuilder.setFailureHandler(new CustomFailureHandler("dashboard", "json"));

		// configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
		// esSinkBuilder.setBulkFlushMaxActions(1);



		// finally, build and add the sink to the job's pipeline
		tripDataStream.addSink(esSinkBuilder.build());

		tripDataStream.print().setParallelism(1);
		env.execute("Smartera challenge");
	}

	private static class CustomFailureHandler implements ActionRequestFailureHandler {

		private static final long serialVersionUID = 942269087742453482L;

		private final String index;
		private final String type;

		CustomFailureHandler(String index, String type) {
			this.index = index;
			this.type = type;
		}

		@Override
		public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
			if (action instanceof IndexRequest) {
				Map<String, Object> json = new HashMap<>();
				json.put("data", ((IndexRequest) action).source());
				indexer.add(
						Requests.indexRequest()
								.index(index)
								.type(type)
								.id(((IndexRequest) action).id())
								.source(json));
			} else {
				throw new IllegalStateException("unexpected");
			}
		}
	}
}