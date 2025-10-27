Client for connecting to Kafka

# How to include this library in your project.

Add the dependency on the maven pom.xml

```
<dependency>
    <groupId>com.biit-solutions</groupId>
    <artifactId>kafka-client</artifactId>
    <version>${kafka-client.version}</version>
</dependency>
```

And remember to include all these beans in your application:

```
@ComponentScan({"...", "com.biit.kafka"})
```

# Event structure

A basic event has the next fields:

**id** The id of the event.

**to** If you want to send only to a specific service when multiples are subscribed to the same topic.

**replyTo** If later an event as an answer is generated, where it must be addressed.

**sessionId** When a message is replayed, the system will return a new message where correlationId will be the request
messageId and the sessionId will be the same sessionId as the request one.

**messageId** When a message is replayed, the system will return a new message where correlationId will be the request
messageId and the sessionId will be the same sessionId as the request one.

**correlationId** When a message is replayed, the system will return a new message where correlationId will be the
request messageId and the sessionId will be the same sessionId as the request one.

**subject** The
purpose: `CREATE`, `UPDATE`, `DELETE`, `ENABLE`, `DISABLE`, `CREATED`, `UPDATED`, `DELETED`, `ENABLED`, `DISABLED`, `ERROR`, `SUCCESS`.

**tag**  A string that identifies the content. Can be a form label, a customer email, etc.

**Tenant** Reserved for multi-tenancy applications. Not used yet.

**contentType** The payload codification. I.e. `application/json`

**authorization** User's authorization token in case some services require action.

**createdAt** The time of creation of the event.

**createdBy**  The actor that creates the event.

**entityType** The class name that contains the payload.
i.e. `com.biit.drools.baseform.core.controllers.kafka.payloads.DroolsVariablePayload`

**payload** The content of the event.

**organization** The organization where the user pertains to.

**unit** Related to a team, department or any other group of users.

**customProperties** a Map to include any software-specific feature that is not described above.

## Customize your specific events.

### Message

Generate an entity with the information you want to send as an event. We will call it `MyPayload` and must implement
the EventPayload interface:

```
public class MyPayload implements EventPayload {
   private String value;
   [...]
}
```

Remember to include all getters and setters, as are required for serialization.

### Producer

For sending an event, we need to use the KafkaTemplate.

```

    @Autowired
    private KafkaEventTemplate kafkaTemplate;

    kafkaTemplate.send(new MyPayload(value));
```

### Consumer

For reading the events that are send through Kafka Streams, subscribe to the existing listener `EventListener` that is
listening to the topic defined at `${spring.kafka.topic}`:

```
    @Autowired
    private EventListener eventListener;
    
     eventListener.addListener(event -> this.myPayload = event.getEntity(MyPayload.class));
```

And you can do whatever fits your need with the listener. If you have an event that is subscribed to multiple different
events with different payloads, you can have problems with the class. If it is the case, use the parameter `entityType`
to filter them and assign the correct class:

```
        eventListener.addListener(event -> {
            if (Objects.equals(event.getEntityType(), MyPayload.class.getName())) {
                this.myPayload = event.getEntity(MyPayload.class);
            }
        });
```

Note that `getEntityType()` can be a class name (as in the previous example) or any other tag used on the application to
distinguish between events.

### Using more than one topic

If you need to subscribe to more than one topic, you can extend the class and redefine the topic needed:

```
@ConditionalOnExpression("!T(org.springframework.util.StringUtils).isEmpty('${spring.kafka.topic:}')")
@EnableKafka
@Configuration
public class MyNewEventListener extends EventListener {

    @Override
    @KafkaListener(topics = "aDifferentTopicName", 
            clientIdPrefix = "#{'${spring.kafka.client.id}'?:T(java.util.UUID).randomUUID().toString()}",
            groupId = "#{'${spring.kafka.group.id}'?:T(java.util.UUID).randomUUID().toString()}",
            containerFactory = "templateEventListenerContainerFactory", autoStartup = "${spring.kafka.enabled:true}")
    public void eventsListener(Event event,
                               final @Header(KafkaHeaders.OFFSET) Integer offset,
                               final @Header(KafkaHeaders.GROUP_ID) String groupId,
                               final @Header(value = KafkaHeaders.KEY, required = false) String key,
                               final @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                               final @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               final @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timeStamp) {
        super.eventListener(event, offset, groupId, key, partition, topic, timeStamp);
    }
}
```

The `@ConditionalOnExpression` ensures that kafka es only enabled if the property `spring.kafka.topic` is set.

##### Autogenerate the new topic

If you have created a different topic, remember to add it to kafka or generate a bean that will generate when starting
the application:

```
    /**
     * With AdminClient from Kafka, the topics are generated programmatically as a bean.
     *
     * @return
     */
    @Bean
    public NewTopic createTopic() {
        return TopicBuilder.name("aDifferentTopicName")
                .partitions(PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }
```

## Update your configuration

Now, set the configuration of your project in your `applications.properties`.

```
spring.kafka.enabled=true
spring.kafka.topic=<Your Topic>
spring.kafka.client.id=
spring.kafka.bootstrap-servers=PLAINTEXT://kafka-server:29092
spring.kafka.group.id=<Your Group>
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=com.biit.kafka.events.EventSerializer
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
spring.kafka.consumer.value-deserializer=com.biit.kafka.events.EventDeserializer
```

# Working with BiiTRestServer project

If you have a controller that extends `ElementController` from BiiTRestServer, you can now extend
the `KafkaElementController` and automatically will generate the events for the CRUD actions.

You will need to generate a Bean that implements `IEventSender` to define how to send these events.

## Testing

If you are importing this library, a Kafka client will be automatically generated. This also will occur on testing, that
can delay each test class around two minutes. If you are not interested on connecting on Kafka on some tests and wants
to speed up the kafka connection, add these settings to your `application.properties`:

```
#Disable Kafka
spring.kafka.enabled=false

#Reduce connection time
spring.kafka.admin.properties[request.timeout.ms]=1000
spring.kafka.admin.properties[default.api.timeout.ms]=1000
```

And if you have a sender, remember to ignore KafkaEventTemplate bean, as will not be created if you disable kafka:

```
public YourEventSender(@Autowired(required = false) KafkaEventTemplate kafkaTemplate) {
    ...
}
```

# Reading Historical data

It is possible to read some historical events. Remember that Kafka by default only stored events around one week.
Therefore, older events cannot be retrieved.

Use  `com.biit.kafka.consumers.HistoricalEventConsumer`:

And you will be able to search events in time range as follows:

```
    @Autowired
    private HistoricalEventConsumer historicalEventConsumer;
    
    ...
    
    historicalEventConsumer.getEvents(startingTimeForSearching, rangeAsDuration);
```

# KSQL

If you want to use KSQL, you need to generate a stream from a set of events using a syntax similar to SQL. Use the
KsqlClientTest as a reference.

# Encrypt events payload.

You can enable the encryption of the event payload, by setting on `application.properties` the next property:

```
kafka.encryption.key=<your secret key>
```

# Logging information

You can enable the log of this library adding the next logger into your `logback.xml`

```
<logger name="com.biit.kafka.logger.KafkaLogger" additivity="false" level="DEBUG">
    <appender-ref ref="DAILY"/>
    <appender-ref ref="CONSOLE"/>
</logger>
``
