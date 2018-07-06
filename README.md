# kafka-springboot-rest
Simple example apache kafka using springboot and rest

1. Download and install Kafka, please refer to the official guide [Here](https://kafka.apache.org/quickstart).

2. Run the application using maven.  
	From your terminal, go to application root directory, which is 'mykafka-simple-rest' then run the application using  
	mvn spring-boot:run

3. Sending message from producer.  
	Open your browser and type http://localhost:8080/trim/kafka/producer?data=Test message
	
4. Receive message on consumer  
	Open new tab on your browser then type http://localhost:8080/trim/kafka/consumer
