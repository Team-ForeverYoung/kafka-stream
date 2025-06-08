package com.java;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Main {

	private static final String BOOTSTRAP_SERVER = "kafka.kafka.svc.cluster.local:9092";
	private static final String SOURCE_TOPIC = "forever_mysql_db.forever_mysql_db.outbox_event";
	private static final String STREAM_SERVER_ID = "outbox-event-router-server";
	private static final String TOPIC_1 = "promotion_event";
	private static final String TOPIC_2 = "promotion_result";
	private static final Logger log = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {

		ObjectMapper objectMapper = new ObjectMapper();
		Properties config = new Properties();
		config.put(StreamsConfig.APPLICATION_ID_CONFIG, STREAM_SERVER_ID);
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder streamsBuilder = new StreamsBuilder();
		KStream<String, String> source = streamsBuilder.stream(SOURCE_TOPIC);

		source.filter((key, value) -> {
			try {
				JsonNode jsonNode = objectMapper.readTree(value);
				boolean match = TOPIC_1.equals(jsonNode.path("topic").asText());
				if (match) {
					log.info("[{}] 라우팅: {}", TOPIC_1, value);
				}
				return match;
			} catch (Exception e) {
				log.error("JSON 파싱 실패 ({}): {}", TOPIC_1, value, e);
				return false;
			}
		}).to(TOPIC_1);

		source.filter((key, value) -> {
			try {
				JsonNode jsonNode = objectMapper.readTree(value);
				boolean match = TOPIC_2.equals(jsonNode.path("topic").asText());
				if (match) {
					log.info("[{}] 라우팅: {}", TOPIC_2, value);
				}
				return match;
			} catch (Exception e) {
				log.error("JSON 파싱 실패 ({}): {}", TOPIC_2, value, e);
				return false;
			}
		}).to(TOPIC_2);

		Topology topology = streamsBuilder.build();
		KafkaStreams streams = new KafkaStreams(topology, config);

		log.info("Kafka Streams 애플리케이션 시작");
		streams.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("Kafka Streams 애플리케이션 종료");
			streams.close();
		}));
	}
}
