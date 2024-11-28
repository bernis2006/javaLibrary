package com.trim.kafkarest.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaProducer.class);
	
	@Autowired
	private final KafkaTemplate<String,String> kafkaTemplate;

	@Value("${spring.kafka.producer.topics.producers}")
	String mytopic = "nrt_enr_rel_BGDTREF";

	public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

	public void send(String keyId, String data){
		LOG.info("Sending data : {} with key: {}", data, keyId);
		kafkaTemplate.send(mytopic,keyId,data);
	}

	public void sendWithoutKey(String data){
		LOG.info("Sending data : {}", data);
		kafkaTemplate.send(mytopic,data);
	}

}
