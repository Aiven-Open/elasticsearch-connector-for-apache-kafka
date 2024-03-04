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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import io.aiven.connect.elasticsearch.bulk.BulkProcessor;
import io.aiven.connect.elasticsearch.clientwrapper.ElasticsearchClientWrapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchSinkTask.class);
    private ElasticsearchWriter writer;
    private ElasticsearchClient client;

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(final Map<String, String> props) {
        start(props, null);
    }

    @SuppressWarnings("deprecation") //TOPIC_INDEX_MAP_CONFIG
    // public for testing
    public void start(final Map<String, String> props, final ElasticsearchClient client) {
        try {
            log.info("Starting ElasticsearchSinkTask.");

            final ElasticsearchSinkConnectorConfig config = new ElasticsearchSinkConnectorConfig(props);
            final String type = config.getString(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG);
            final boolean ignoreKey =
                config.getBoolean(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG);
            final boolean ignoreSchema =
                config.getBoolean(ElasticsearchSinkConnectorConfig.SCHEMA_IGNORE_CONFIG);
            final boolean useCompactMapEntries =
                config.getBoolean(ElasticsearchSinkConnectorConfig.COMPACT_MAP_ENTRIES_CONFIG);


            final Map<String, String> topicToIndexMap =
                parseMapConfig(config.getList(ElasticsearchSinkConnectorConfig.TOPIC_INDEX_MAP_CONFIG));
            final Set<String> topicIgnoreKey =
                new HashSet<>(config.getList(ElasticsearchSinkConnectorConfig.TOPIC_KEY_IGNORE_CONFIG));
            final Set<String> topicIgnoreSchema = new HashSet<>(
                config.getList(ElasticsearchSinkConnectorConfig.TOPIC_SCHEMA_IGNORE_CONFIG)
            );

            final long flushTimeoutMs =
                config.getLong(ElasticsearchSinkConnectorConfig.FLUSH_TIMEOUT_MS_CONFIG);
            final int maxBufferedRecords =
                config.getInt(ElasticsearchSinkConnectorConfig.MAX_BUFFERED_RECORDS_CONFIG);
            final int batchSize =
                config.getInt(ElasticsearchSinkConnectorConfig.BATCH_SIZE_CONFIG);
            final long lingerMs =
                config.getLong(ElasticsearchSinkConnectorConfig.LINGER_MS_CONFIG);
            final int maxInFlightRequests =
                config.getInt(ElasticsearchSinkConnectorConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG);
            final long retryBackoffMs =
                config.getLong(ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG);
            final int maxRetry =
                config.getInt(ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG);
            final boolean dropInvalidMessage =
                config.getBoolean(ElasticsearchSinkConnectorConfig.DROP_INVALID_MESSAGE_CONFIG);

            final DataConverter.BehaviorOnNullValues behaviorOnNullValues =
                DataConverter.BehaviorOnNullValues.forValue(
                    config.getString(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG)
                );

            final BulkProcessor.BehaviorOnMalformedDoc behaviorOnMalformedDoc =
                BulkProcessor.BehaviorOnMalformedDoc.forValue(
                    config.getString(ElasticsearchSinkConnectorConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG)
                );

            // Calculate the maximum possible backoff time ...
            final long maxRetryBackoffMs =
                RetryUtil.computeRetryWaitTimeInMillis(maxRetry, retryBackoffMs);
            if (maxRetryBackoffMs > RetryUtil.MAX_RETRY_TIME_MS) {
                log.warn("This connector uses exponential backoff with jitter for retries, "
                        + "and using '{}={}' and '{}={}' results in an impractical but possible maximum "
                        + "backoff time greater than {} hours.",
                    ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG, maxRetry,
                    ElasticsearchSinkConnectorConfig.RETRY_BACKOFF_MS_CONFIG, retryBackoffMs,
                    TimeUnit.MILLISECONDS.toHours(maxRetryBackoffMs));
            }

            if (client != null) {
                this.client = client;
            } else {
                this.client = new ElasticsearchClientWrapper(config);
            }

            final ElasticsearchWriter.Builder builder = new ElasticsearchWriter.Builder(this.client)
                .setType(type)
                .setIgnoreKey(ignoreKey, topicIgnoreKey)
                .setIgnoreSchema(ignoreSchema, topicIgnoreSchema)
                .setCompactMapEntries(useCompactMapEntries)
                .setTopicToIndexMap(topicToIndexMap)
                .setFlushTimoutMs(flushTimeoutMs)
                .setMaxBufferedRecords(maxBufferedRecords)
                .setMaxInFlightRequests(maxInFlightRequests)
                .setBatchSize(batchSize)
                .setLingerMs(lingerMs)
                .setRetryBackoffMs(retryBackoffMs)
                .setMaxRetry(maxRetry)
                .setDropInvalidMessage(dropInvalidMessage)
                .setBehaviorOnNullValues(behaviorOnNullValues)
                .setBehaviorOnMalformedDoc(behaviorOnMalformedDoc);

            writer = builder.build();
            writer.start();
        } catch (final ConfigException e) {
            throw new ConnectException(
                "Couldn't start ElasticsearchSinkTask due to configuration error:",
                e
            );
        }
    }

    @Override
    public void open(final Collection<TopicPartition> partitions) {
        log.debug("Opening the task for topic partitions: {}", partitions);
        final Set<String> topics = new HashSet<>();
        for (final TopicPartition tp : partitions) {
            topics.add(tp.topic());
        }
        writer.createIndicesForTopics(topics);
    }

    @Override
    public void put(final Collection<SinkRecord> records) throws ConnectException {
        log.trace("Putting {} to Elasticsearch.", records);
        writer.write(records);
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        log.trace("Flushing data to Elasticsearch with the following offsets: {}", offsets);
        writer.flush();
    }

    @Override
    public void close(final Collection<TopicPartition> partitions) {
        log.debug("Closing the task for topic partitions: {}", partitions);
    }

    public void refresh(final String index) throws IOException {
        client.refreshIndex(index);
    }

    @Override
    public void stop() throws ConnectException {
        log.info("Stopping ElasticsearchSinkTask.");
        if (writer != null) {
            writer.stop();
        }
        if (client != null) {
            try {
                client.close();
            } catch (final IOException e) {
                throw new ConnectException(e);
            }
        }
    }

    private Map<String, String> parseMapConfig(final List<String> values) {
        final Map<String, String> map = new HashMap<>();
        for (final String value : values) {
            final String[] parts = value.split(":");
            final String topic = parts[0];
            final String type = parts[1];
            map.put(topic, type);
        }
        return map;
    }

}
