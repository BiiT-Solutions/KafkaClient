package com.biit.kafka.tests;

import com.biit.kafka.config.KafkaConfig;
import com.biit.kafka.ksql.KsqlClient;
import com.biit.kafka.ksql.RowSubscriber;
import com.biit.kafka.ksql.entities.Alert;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.given;

@SpringBootTest
@Test(groups = {"ksqlClient"})
public class KsqlClientTest extends AbstractTestNGSpringContextTests {

    private static final Map<String, Object> PROPERTIES = Collections.singletonMap("auto.offset.reset", "earliest");

    private static final String ALERTS_QUERY = "SELECT * FROM alerts EMIT CHANGES;";

    private static final String READINGS_STREAM = "readings";

    private static final String CREATE_READINGS_STREAM = """
            CREATE STREAM IF NOT EXISTS readings (sensor_id VARCHAR KEY, timestamp VARCHAR, reading INT)
            WITH (KAFKA_TOPIC = 'readings',
                VALUE_FORMAT = 'JSON',
                TIMESTAMP = 'timestamp',
                TIMESTAMP_FORMAT = 'yyyy-MM-dd HH:mm:ss',
                PARTITIONS = 1);
            """;

    private static final String CREATE_ALERTS_TABLE = """
            CREATE TABLE IF NOT EXISTS alerts AS
              SELECT
                sensor_id,
                TIMESTAMPTOSTRING(WINDOWSTART, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS start_period,
                TIMESTAMPTOSTRING(WINDOWEND, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS end_period,
                AVG(reading) AS average_reading
            FROM readings
            WINDOW TUMBLING (SIZE 30 MINUTES)
            GROUP BY sensor_id
            HAVING AVG(reading) > 25
            EMIT CHANGES;
            """;


    @Autowired
    private KsqlClient client;

    @Autowired
    private KafkaConfig kafkaConfig;

    private RowSubscriber<Alert> alertSubscriber;

    private void deleteAlerts() {
        client.listQueries()
                .thenApply(queryInfos -> queryInfos.stream()
                        .filter(queryInfo -> queryInfo.getQueryType() == QueryInfo.QueryType.PERSISTENT)
                        .map(QueryInfo::getId)
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException("Persistent query not found")))
                .thenCompose(id -> client.executeStatement("TERMINATE " + id + ";"))
                .thenCompose(result -> client.executeStatement("DROP TABLE IF EXISTS alerts DELETE TOPIC;"))
                .thenCompose(result -> client.executeStatement("DROP STREAM IF EXISTS readings DELETE TOPIC;"))
                .join();
    }

    private Alert expectedAlert(String sensorId, String startPeriod, String endPeriod, double average) {
        return new Alert(sensorId, startPeriod, endPeriod, average);
    }

    private void createAlertsMaterializedView() {
        client.executeStatement(CREATE_READINGS_STREAM, PROPERTIES).join();
        client.executeStatement(CREATE_ALERTS_TABLE, PROPERTIES).join();
    }

    private void insertSampleData() {
        client.insert(READINGS_STREAM,
                Arrays.asList(
                        new KsqlObject().put("sensor_id", "sensor-1").put("timestamp", "2023-05-24 09:00:00").put("reading", 22),
                        new KsqlObject().put("sensor_id", "sensor-1").put("timestamp", "2023-05-24 09:10:00").put("reading", 20),
                        new KsqlObject().put("sensor_id", "sensor-1").put("timestamp", "2023-05-24 09:20:00").put("reading", 20),

                        // these reading will exceed the alert threshold (sensor-1)
                        new KsqlObject().put("sensor_id", "sensor-1").put("timestamp", "2023-05-24 09:30:00").put("reading", 24),
                        new KsqlObject().put("sensor_id", "sensor-1").put("timestamp", "2023-05-24 09:40:00").put("reading", 30),
                        new KsqlObject().put("sensor_id", "sensor-1").put("timestamp", "2023-05-24 09:50:00").put("reading", 30),

                        new KsqlObject().put("sensor_id", "sensor-1").put("timestamp", "2023-05-24 10:00:00").put("reading", 24),

                        // these reading will exceed the alert threshold (sensor-2)
                        new KsqlObject().put("sensor_id", "sensor-2").put("timestamp", "2023-05-24 10:00:00").put("reading", 26),
                        new KsqlObject().put("sensor_id", "sensor-2").put("timestamp", "2023-05-24 10:10:00").put("reading", 26),
                        new KsqlObject().put("sensor_id", "sensor-2").put("timestamp", "2023-05-24 10:20:00").put("reading", 26),

                        new KsqlObject().put("sensor_id", "sensor-1").put("timestamp", "2023-05-24 10:30:00").put("reading", 24)
                )
        ).join();
    }

    @BeforeClass
    private void cleanStreams() {
        try {
            deleteAlerts();
        } catch (Exception ignored) {

        }
    }

    @BeforeClass(dependsOnMethods = "cleanStreams")
    private void prepareStreams() {
        createAlertsMaterializedView();
        alertSubscriber = new RowSubscriber<>(Alert.class);

        CompletableFuture<Void> result = client.subscribe(ALERTS_QUERY, alertSubscriber, PROPERTIES);
        Assert.assertNotNull(result);
        insertSampleData();
    }

    @Test
    void givenSensorReadings_whenSubscribedToAlerts_thenAlertsAreConsumed() {
        await().atMost(Duration.ofMinutes(3)).untilAsserted(() ->
                Assert.assertTrue(alertSubscriber.consumedItems
                        .containsAll(Arrays.asList(
                                expectedAlert("sensor-1", "2023-05-24 09:30:00", "2023-05-24 10:00:00", 28.0),
                                expectedAlert("sensor-2", "2023-05-24 10:00:00", "2023-05-24 10:30:00", 26.0)
                        ))));
    }


    @Test
    void givenSensorReadings_whenPullQueryForRow_thenRowIsReturned() {
        String pullQuery = "SELECT * FROM alerts WHERE sensor_id = 'sensor-2';";

        //This test can take few minutes to finish
        given().ignoreExceptions()
                .await().atMost(Duration.ofMinutes(1))
                .untilAsserted(() -> {
                    // it may be possible that the materialized view is not updated with sample data yet
                    // so ignore TimeoutException and try again
                    List<Row> rows = client.executeQuery(pullQuery, PROPERTIES)
                            .get(10, TimeUnit.SECONDS);

                    Assert.assertEquals(rows.size(), 1);

                    Row row = rows.get(0);
                    Assert.assertEquals(row.getString("SENSOR_ID"), "sensor-2");
                    Assert.assertEquals(row.getString("START_PERIOD"), "2023-05-24 10:00:00");
                    Assert.assertEquals(row.getString("END_PERIOD"), "2023-05-24 10:30:00");
                    Assert.assertEquals(row.getDouble("AVERAGE_READING"), 26.0);
                });
    }

    @AfterClass(alwaysRun = true)
    private void tearDown() {
        deleteAlerts();
    }

}
