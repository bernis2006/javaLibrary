package com.trim.kafkarest.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.trim.kafkarest.services.KafkaProducer;
import com.trim.kafkarest.storages.MessageStorage;

@RestController
@RequestMapping(value = "trim/kafka")
public class WebRestController {

	private static final Logger LOG = LoggerFactory
			.getLogger(WebRestController.class);

	@Autowired
	KafkaProducer kafkaProducer;

	@Autowired
	MessageStorage storage;

	@GetMapping(value = "/producer")
	public String producer(@RequestParam("data") String data) {
		kafkaProducer.send(data);
		LOG.info("data value : {}", data);
		LOG.info("storage value : {}", storage.toString());
		return "Done";
	}

	@GetMapping(value = "/consumer")
	public String consumer() {
		String messages = storage.toString();
		storage.clear();

		return messages;
	}

}
