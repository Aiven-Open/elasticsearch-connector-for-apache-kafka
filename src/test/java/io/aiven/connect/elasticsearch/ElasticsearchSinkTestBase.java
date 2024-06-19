/*
 * Copyright 2020 Aiven Oy
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.connect.elasticsearch;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.connect.elasticsearch.clientwrapper.ElasticsearchClientWrapper;

import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import static org.junit.Assert.assertEquals;

public class ElasticsearchSinkTestBase {

    private static final String DEFAULT_ELASTICSEARCH_TEST_CONTAINER_VERSION = "8.13.0";
    private static final String ELASTICSEARCH_PASSWORD = "disable_tls_for_testing";
    protected static final String TYPE = "kafka-connect";

    private static final String TOPIC = "topic";
    protected static final int PARTITION = 12;
    protected static final int PARTITION2 = 13;
    protected static final int PARTITION3 = 14;
    protected ElasticsearchClient client;
    private DataConverter converter;

    private static ElasticsearchContainer container;

    private final String testTopic;

    public ElasticsearchSinkTestBase() {
        this.testTopic = String.format("%s-%s", TOPIC, UUID.randomUUID());
    }

    @BeforeClass
    public static void staticSetUp() {
        final String elasticsearchContainerVersion = System.getenv().getOrDefault(
            "ELASTIC_TEST_CONTAINER_VERSION",
            DEFAULT_ELASTICSEARCH_TEST_CONTAINER_VERSION
        );
        container = new ElasticsearchContainer("elasticsearch:" + elasticsearchContainerVersion)
            .withPassword(ELASTICSEARCH_PASSWORD);
        container.getEnvMap().put("xpack.security.transport.ssl.enabled", "false");
        container.getEnvMap().put("xpack.security.http.ssl.enabled", "false");
        container.getEnvMap().put("xpack.security.enabled", "false");
        container.setWaitStrategy(new LogMessageWaitStrategy()
            .withRegEx(getLogMessageWaitStrategyRegex(elasticsearchContainerVersion)));
        container.start();
    }

    private static String getLogMessageWaitStrategyRegex(final String elasticContainerVersion) {
        final char majorVersion = elasticContainerVersion.charAt(0);
        switch (majorVersion) {
            case '7':
                return ".*\"message\": \"started.*";
            case '8':
                return ".*\"message\":\"started.*";
            default:
                throw new IllegalArgumentException(
                    String.format("Supported major versions are 7 and 8, argument resolved to major version %s",
                        majorVersion));
        }
    }

    @Before
    public void setUp() throws Exception {
        final Map<String, String> props = new HashMap<>();
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://" + container.getHttpHostAddress());
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG, ELASTICSEARCH_PASSWORD);
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG, "elastic");
        props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, "kafka-connect");
        final ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
        client = new ElasticsearchClientWrapper(config);
        converter = new DataConverter(true, DataConverter.BehaviorOnNullValues.IGNORE);
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        client = null;
    }

    @AfterClass
    public static void staticTearDown() {
        if (container != null) {
            container.close();
        }
        container = null;
    }

    public String getTopic() {
        return this.testTopic;
    }

    public TopicPartition getTopicPartition(final int partition) {
        return new TopicPartition(this.testTopic, partition);
    }

    protected Struct createRecord(final Schema schema) {
        final Struct struct = new Struct(schema);
        struct.put("user", "Liquan");
        struct.put("message", "trying out Elastic Search.");
        return struct;
    }

    protected Schema createSchema() {
        return SchemaBuilder.struct().name("record")
            .field("user", Schema.STRING_SCHEMA)
            .field("message", Schema.STRING_SCHEMA)
            .build();
    }

    protected Schema createOtherSchema() {
        return SchemaBuilder.struct().name("record")
            .field("user", Schema.INT32_SCHEMA)
            .build();
    }

    protected Struct createOtherRecord(final Schema schema) {
        final Struct struct = new Struct(schema);
        struct.put("user", 10);
        return struct;
    }

    protected void verifySearchResults(
        final Collection<SinkRecord> records,
        final boolean ignoreKey,
        final boolean ignoreSchema) throws IOException {
        verifySearchResults(records, getTopic(), ignoreKey, ignoreSchema);
    }

    protected void verifySearchResults(
        final Collection<?> records,
        final String index,
        final boolean ignoreKey,
        final boolean ignoreSchema) throws IOException {
        final SearchResponse<JsonNode> result = client.search(index);
        final List<Hit<JsonNode>> rawHits = result.hits().hits();

        assertEquals(records.size(), rawHits.size());

        final Map<String, String> hits = new HashMap<>();
        for (final Hit<JsonNode> hit: rawHits) {
            final String id = hit.id();
            final String source = hit.source().toString();
            hits.put(id, source);
        }

        for (final Object record : records) {
            if (record instanceof SinkRecord) {
                final IndexableRecord indexableRecord =
                    converter.convertRecord((SinkRecord) record, index, TYPE, ignoreKey, ignoreSchema);
                assertEquals(indexableRecord.payload, hits.get(indexableRecord.key.id));
            } else {
                assertEquals(record, hits.get("key"));
            }
        }
    }
}
