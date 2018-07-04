package com.trim.kafkarest.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.trim.kafkarest.storages.MessageStorage;

@Component
public class KafkaConsumer {

	private static final Logger LOG = LoggerFactory
			.getLogger(KafkaConsumer.class);

	@Autowired
	MessageStorage messageStorage;

	@KafkaListener(topics = "${app.kafka.topic}")
	public void processMessage(String content) {
		LOG.info("Receive content : {} ", content);
		messageStorage.put(content);
	}
}
