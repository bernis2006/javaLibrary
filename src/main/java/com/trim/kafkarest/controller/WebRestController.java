package com.trim.kafkarest.controller;

import com.trim.kafkarest.storages.MessageStorageConsumer;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.web.bind.annotation.*;

import com.trim.kafkarest.services.KafkaProducer;
import com.trim.kafkarest.storages.MessageStorage;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;


@CrossOrigin(origins = "http://localhost:5500")
@RestController
@RequestMapping(value = "trim/kafka")
public class WebRestController {

	private static final Logger LOG = LoggerFactory
			.getLogger(WebRestController.class);

	@Autowired
	private ConsumerFactory<String, String> consumerFactory;

	@Autowired
	KafkaProducer kafkaProducer;

	@Autowired
	MessageStorage storage;

	@Autowired
	MessageStorageConsumer messageStorageConsumer;

	@Autowired
	private AdminClient kafkaAdminClient;  // Cliente de administración de Kafka


	private ConcurrentMessageListenerContainer<String, String> listenerContainer;


	@GetMapping(value = "/producer")
	public String producer(@RequestParam String keyId , @RequestBody String data) {
		kafkaProducer.send(keyId,data);
		LOG.info("data value : {} and keyid: {}" , data, keyId);
		LOG.info("storage value : {}" + storage.toString());
		return "Done";
	}

	@GetMapping(value = "/producerWithoutKey")
	public String producerWithoutKey(@RequestBody String data) {
		kafkaProducer.sendWithoutKey(data);
		LOG.info("data value : {}", data);
		LOG.info("storage value : {}" , storage);
		return "Done";
	}


	@PostMapping(value = "/consumer")
	public String startConsumer(@RequestParam String topic) {
		// Detener cualquier listener anterior
		if (listenerContainer != null) {
			listenerContainer.stop();
		}

		// Crear las propiedades del contenedor para escuchar el tópico específico
		ContainerProperties containerProps = new ContainerProperties(topic);

		// Establecer el listener para procesar los mensajes entrantes
		containerProps.setMessageListener((MessageListener<String, String>) record -> {
			// Extraer keyId y timestamp del mensaje
			String keyId = record.key();
			long timestamp = record.timestamp();

			// Convertir timestamp de UNIX a fecha legible
			String formattedDate = Instant.ofEpochMilli(timestamp)
					.atZone(ZoneId.systemDefault())  // Usa la zona horaria local
					.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

			// Mostrar el mensaje recibido con fecha legible
			LOG.info("Mensaje recibido -> KeyId: {}, Timestamp: {}", keyId, formattedDate);

			// Almacenar solo los últimos mensajes (si es necesario, puedes agregar lógica de almacenamiento más avanzada)
			messageStorageConsumer.add("KeyId: " + keyId + ", Timestamp: " + formattedDate);
		});

		// Crear el contenedor de escucha y empezar a consumir
		listenerContainer = new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
		listenerContainer.start();

		return "Consumer started for topic: " + topic;
	}

	// Endpoint para obtener los mensajes almacenados
	@GetMapping(value = "/messages")
	public String getMessages() {
		// Solo devuelve los mensajes recientes almacenados en el MessageStorage
		return messageStorageConsumer.toString();
	}

	@PostMapping(value = "/consumerLag")
	public String getConsumerLag(@RequestParam String groupId) {
		try {
			// Obtener la descripción del grupo de consumidores
			DescribeConsumerGroupsResult describeResult = kafkaAdminClient.describeConsumerGroups(List.of(groupId));
			ConsumerGroupDescription groupDescription = describeResult.all().get().get(groupId);

			// Recuperar las particiones que el grupo de consumidores está consumiendo
			Set<TopicPartition> partitions = groupDescription.members().stream()
					.flatMap(member -> member.assignment().topicPartitions().stream())
					.collect(Collectors.toSet());

			// Obtener los offsets más recientes para cada partición
			Map<TopicPartition, OffsetSpec> latestOffsetsSpec = partitions.stream()
					.collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

			ListOffsetsResult latestOffsetsResult = kafkaAdminClient.listOffsets(latestOffsetsSpec);
			Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = latestOffsetsResult.all().get();

			// Obtener los offsets consumidos por el grupo de consumidores
			Map<TopicPartition, OffsetAndMetadata> consumerOffsets = kafkaAdminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

			// Crear una respuesta con el lag para cada partición
			StringBuilder lagInfo = new StringBuilder("Consumer Lag for group " + groupId + ":\n");

			for (TopicPartition partition : partitions) {
				OffsetAndMetadata consumerOffset = consumerOffsets.get(partition);
				long currentOffset = (consumerOffset != null) ? consumerOffset.offset() : 0L;
				long latestOffset = latestOffsets.get(partition).offset();

				// Comprobar si los offsets están correctamente obtenidos
				if (consumerOffset == null) {
				//	lagInfo.append(String.format("No offset found for group %s in topic %s, partition %d\n", groupId, partition.topic(), partition.partition()));
					continue;
				}

				long lag = latestOffset - currentOffset;
				lagInfo.append(String.format("Topic: %s, Partition: %d, Lag: %d, Current Offset: %d, Latest Offset: %d\n",
						partition.topic(), partition.partition(), lag, currentOffset, latestOffset));
			}

			return lagInfo.toString();
		} catch (ExecutionException | InterruptedException e) {
			LOG.error("Error al obtener el lag del consumidor: ", e);
			return "Error al obtener el lag del consumidor.";
		}
	}

	@PostMapping(value = "/consumerMuiltipleLag")
	public String getConsumerLag(@RequestParam List<String> groupIds) {
		StringBuilder lagInfo = new StringBuilder();
		try {
			for (String groupId : groupIds) {
				// Obtener la descripción del grupo de consumidores
				DescribeConsumerGroupsResult describeResult = kafkaAdminClient.describeConsumerGroups(List.of(groupId));
				ConsumerGroupDescription groupDescription = describeResult.all().get().get(groupId);

				// Recuperar las particiones que el grupo de consumidores está consumiendo
				Set<TopicPartition> partitions = groupDescription.members().stream()
						.flatMap(member -> member.assignment().topicPartitions().stream())
						.collect(Collectors.toSet());

				// Obtener los offsets más recientes para cada partición
				Map<TopicPartition, OffsetSpec> latestOffsetsSpec = partitions.stream()
						.collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));

				ListOffsetsResult latestOffsetsResult = kafkaAdminClient.listOffsets(latestOffsetsSpec);
				Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = latestOffsetsResult.all().get();

				// Obtener los offsets consumidos por el grupo de consumidores
				Map<TopicPartition, OffsetAndMetadata> consumerOffsets = kafkaAdminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

				// Agregar información del lag del grupo actual
				lagInfo.append("Consumer Lag for group ").append(groupId).append(":\n");

				for (TopicPartition partition : partitions) {
					OffsetAndMetadata consumerOffset = consumerOffsets.get(partition);
					long currentOffset = (consumerOffset != null) ? consumerOffset.offset() : 0L;
					long latestOffset = latestOffsets.get(partition).offset();

					if (consumerOffset == null) {
					//	lagInfo.append(String.format("No offset found for group %s in topic %s, partition %d\n", groupId, partition.topic(), partition.partition()));
						continue;
					}

					long lag = latestOffset - currentOffset;
					lagInfo.append(String.format("Topic: %s, Partition: %d, Lag: %d, Current Offset: %d, Latest Offset: %d\n",
							partition.topic(), partition.partition(), lag, currentOffset, latestOffset));
				}
				lagInfo.append("\n");
			}
		} catch (ExecutionException | InterruptedException e) {
			LOG.error("Error al obtener el lag del consumidor: ", e);
			return "Error al obtener el lag del consumidor.";
		}
		return lagInfo.toString();
	}


}
