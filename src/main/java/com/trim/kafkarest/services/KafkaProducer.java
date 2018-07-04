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
	private KafkaTemplate<String,String> kafkaTemplate;
	
	@Value("${app.kafka.topic}")
	String mytopic = "mytopic";
	
	public void send(String data){
		LOG.info("Sending data : {} ", data);
		kafkaTemplate.send(mytopic, data);
	}

}
