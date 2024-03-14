/*
 * Copyright 2024 Aiven Oy
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import io.aiven.connect.elasticsearch.bulk.BulkResponse;
import io.aiven.connect.elasticsearch.clientwrapper.ElasticsearchClientWrapper;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


public class ElasticsearchSinkFailureTest {

    @Test
    public void testRetryIfRecoverable() throws IOException {
        final ElasticsearchSinkTask elasticsearchSinkTask = new ElasticsearchSinkTask();
        final int numbRetriesBeforeSucceeding = 3;

        final ElasticsearchClientWrapper failingClient = Mockito.mock(ElasticsearchClientWrapper.class);
        final AtomicInteger apiCallCounter = new AtomicInteger(0);
        when(failingClient.executeBulk(any())).thenAnswer(i -> {
            final int numAttempt = apiCallCounter.incrementAndGet();
            if (numAttempt < numbRetriesBeforeSucceeding) {
                return BulkResponse.failure(true, "");
            }
            return BulkResponse.success();
        });

        final Map<String, String> props  = new HashMap<>();
        props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, "kafka-connect");
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "localhost");
        props.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
        props.put(ElasticsearchSinkConnectorConfig.DROP_INVALID_MESSAGE_CONFIG, "true");
        props.put(ElasticsearchSinkConnectorConfig.MAX_RETRIES_CONFIG, String.valueOf(numbRetriesBeforeSucceeding));

        elasticsearchSinkTask.start(props, failingClient);
        elasticsearchSinkTask.put(Collections.singletonList(new SinkRecord("topic", 0, null, null, null, "foo", 0)));
        elasticsearchSinkTask.flush(Collections.emptyMap());
        // 1 success
        assertEquals(numbRetriesBeforeSucceeding, apiCallCounter.get());
        elasticsearchSinkTask.put(Collections.singletonList(new SinkRecord("topic", 0, null, null, null, "bar", 0)));
        elasticsearchSinkTask.flush(Collections.emptyMap());
        // 3 retries (the max allowed) + 1 success
        assertEquals(numbRetriesBeforeSucceeding + 1, apiCallCounter.get());
    }

    @Test
    public void testRaiseExceptionIfNot() throws IOException {
        final ElasticsearchSinkTask elasticsearchSinkTask = new ElasticsearchSinkTask();
        final ElasticsearchClientWrapper failingClient = Mockito.mock(ElasticsearchClientWrapper.class);
        final AtomicInteger apiCallCounter = new AtomicInteger(0);
        when(failingClient.executeBulk(any())).thenAnswer(i -> {
            apiCallCounter.incrementAndGet();
            return BulkResponse.failure(false, "");
        });

        final Map<String, String> props = new HashMap<>();
        props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, "kafka-connect");
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "localhost");
        props.put(ElasticsearchSinkConnectorConfig.KEY_IGNORE_CONFIG, "true");
        props.put(ElasticsearchSinkConnectorConfig.DROP_INVALID_MESSAGE_CONFIG, "true");

        elasticsearchSinkTask.start(props, failingClient);
        elasticsearchSinkTask.put(Collections.singletonList(new SinkRecord("topic", 0, null, null, null, "foo", 0)));

        // test that flush throws an exception
        assertThrows(ConnectException.class, () -> elasticsearchSinkTask.flush(Collections.emptyMap()));
        assertEquals(1, apiCallCounter.get());

        // test that the exception is raised also if we try to add another record
        final ConnectException e = assertThrows(ConnectException.class, () -> elasticsearchSinkTask.put(
                Collections.singletonList(
                    new SinkRecord("topic", 0, null, null, null, "bar", 0))
            )
        );
        // the atomic variable isn't changed, the first exception is raised. The connector
        // instance should die after the first exception since kafka connect will restart it
        assertEquals(1, apiCallCounter.get());
    }
}
