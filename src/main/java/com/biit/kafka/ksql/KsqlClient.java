package com.biit.kafka.ksql;

import com.biit.kafka.logger.KafkaLogger;
import io.confluent.ksql.api.client.BatchedQueryResult;
import io.confluent.ksql.api.client.Client;
import io.confluent.ksql.api.client.ClientOptions;
import io.confluent.ksql.api.client.ExecuteStatementResult;
import io.confluent.ksql.api.client.KsqlObject;
import io.confluent.ksql.api.client.QueryInfo;
import io.confluent.ksql.api.client.Row;
import org.reactivestreams.Subscriber;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class KsqlClient {
    private static final Integer DEFAULT_KSQL_PORT = 8088;

    private final Client client;

    public KsqlClient(@Value("${kafka.ksql.host:ksqldb-server}") String ksqlHost,
                      @Value("${kafka.ksql.port:8088}") String ksqlHPort) {
        int definedksqlHPort;
        try {
            definedksqlHPort = Integer.parseInt(ksqlHPort);
        } catch (NumberFormatException e) {
            KafkaLogger.warning(this.getClass(), "Invalid port '" + ksqlHPort + "' defined for Ksql Server.");
            definedksqlHPort = DEFAULT_KSQL_PORT;
        }

        ClientOptions options = ClientOptions.create()
                .setHost(ksqlHost)
                .setPort(definedksqlHPort);

        client = Client.create(options);
    }

    public BatchedQueryResult executeQuery(String query) {
        return client.executeQuery(query);
    }

    public BatchedQueryResult executeQuery(String query, Map<String, Object> properties) {
        return client.executeQuery(query, properties);
    }

    public CompletableFuture<ExecuteStatementResult> executeStatement(String statement) {
        return client.executeStatement(statement);
    }

    public CompletableFuture<ExecuteStatementResult> executeStatement(String statement, Map<String, Object> properties) {
        return client.executeStatement(statement, properties);
    }

    public CompletableFuture<Void> insert(String stream, Collection<KsqlObject> rows) {
        return CompletableFuture.allOf(
                rows.stream()
                        .map(row -> client.insertInto(stream, row))
                        .toArray(CompletableFuture[]::new)
        );
    }

    public CompletableFuture<Void> subscribe(String query, Subscriber<Row> subscriber, Map<String, Object> properties) {
        return client.streamQuery(query, properties)
                .thenAccept(streamedQueryResult -> streamedQueryResult.subscribe(subscriber))
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        KafkaLogger.severe(this.getClass(), "Push query '{}' failed", query);
                        KafkaLogger.errorMessage(this.getClass(), ex);
                    }
                });
    }

    public CompletableFuture<List<QueryInfo>> listQueries() {
        return client.listQueries();
    }
}
