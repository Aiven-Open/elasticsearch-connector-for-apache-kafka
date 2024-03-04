/*
 * Copyright 2020 Aiven Oy
 * Copyright 2018 Confluent Inc.
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

package io.aiven.connect.elasticsearch.clientwrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.elasticsearch.ElasticsearchClient;
import io.aiven.connect.elasticsearch.IndexableRecord;
import io.aiven.connect.elasticsearch.Key;
import io.aiven.connect.elasticsearch.Mapping;
import io.aiven.connect.elasticsearch.bulk.BulkRequest;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ElasticsearchVersionInfo;
import co.elastic.clients.elasticsearch._types.ErrorCause;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import co.elastic.clients.elasticsearch._types.ShardStatistics;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch._types.mapping.TextProperty;
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.InfoResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.OperationType;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.ElasticsearchIndicesClient;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.GetMappingRequest;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticsearchClientWrapperTest {

    private static final String INDEX = "index";
    private static final String KEY = "key";
    private static final String TYPE = "type";

    private ElasticsearchTransport elasticsearchTransport;
    private co.elastic.clients.elasticsearch.ElasticsearchClient elasticsearchClient;
    private ElasticsearchClientWrapper elasticsearchClientWrapper;

    @Before
    public void setUp() throws Exception {
        elasticsearchTransport = mock(ElasticsearchTransport.class);
        elasticsearchClient = mock(co.elastic.clients.elasticsearch.ElasticsearchClient.class);
        when(elasticsearchClient.info()).thenReturn(
            new InfoResponse.Builder()
                .version(new ElasticsearchVersionInfo.Builder()
                    .number("8.0")
                    .buildFlavor("buildFlavor")
                    .buildType("buildType")
                    .buildHash("buildHash")
                    .buildDate("buildDate")
                    .buildSnapshot(false)
                    .luceneVersion("luceneVersion")
                    .minimumWireCompatibilityVersion("minWireCompVersion")
                    .minimumIndexCompatibilityVersion("minIndexCompVersion")
                    .build()
                )
                .clusterName("clusterName")
                .clusterUuid(UUID.randomUUID().toString())
                .tagline("tagLine")
                .name("name")
                .build()
        );
        // InfoResponse required for resolving server version when initializing the client
        elasticsearchClientWrapper = new ElasticsearchClientWrapper(elasticsearchTransport, elasticsearchClient);
    }

    @Test
    public void getsVersion() {
        assertThat(elasticsearchClientWrapper.getVersion(), is(equalTo(ElasticsearchClient.Version.ES_V8)));
    }

    @Test
    public void createsIndices() throws Exception {
        final ElasticsearchIndicesClient indicesClient = mock(ElasticsearchIndicesClient.class);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        when(indicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(false));
        when(indicesClient.create(any(CreateIndexRequest.class))).thenThrow(new IOException("failure"));

        when(indicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(false));
        when(indicesClient.create(argThat(isCreateIndexForTestIndex()))).thenReturn(
            new CreateIndexResponse.Builder()
                .index(INDEX)
                .acknowledged(true)
                .shardsAcknowledged(true)
                .build()
        );

        final Set<String> indices = new HashSet<>();
        indices.add(INDEX);
        elasticsearchClientWrapper.createIndices(indices);
        final InOrder inOrder = inOrder(elasticsearchClient, indicesClient);
        inOrder.verify(elasticsearchClient).info();
        inOrder.verify(indicesClient).exists(any(ExistsRequest.class));
        inOrder.verify(indicesClient).create(argThat(isCreateIndexForTestIndex()));
    }

    private ArgumentMatcher<CreateIndexRequest> isCreateIndexForTestIndex() {
        return new ArgumentMatcher<CreateIndexRequest>() {
            @Override
            public boolean matches(final CreateIndexRequest createIndexRequest) {
                // check the URI as the equals method on CreateIndex doesn't work
                return createIndexRequest.index().equals(INDEX);
            }
        };
    }

    @Test
    public void createIndicesAndFails() throws Exception {
        final ElasticsearchIndicesClient indicesClient = mock(ElasticsearchIndicesClient.class);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        when(indicesClient.exists(any(ExistsRequest.class))).thenReturn(new BooleanResponse(false));
        when(indicesClient.create(any(CreateIndexRequest.class))).thenThrow(new IOException("failure"));

        final Set<String> indices = new HashSet<>();
        indices.add("test-index");
        assertThrows("Could not create index 'test-index'", ConnectException.class, () -> {
            elasticsearchClientWrapper.createIndices(indices);
        });
    }

    @Test
    public void createsMapping() throws Exception {
        final ElasticsearchIndicesClient indicesClient = mock(ElasticsearchIndicesClient.class);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        final ObjectNode obj = JsonNodeFactory.instance.objectNode();
        obj.set(TYPE, Mapping.inferMapping(elasticsearchClientWrapper.getVersion(), Schema.STRING_SCHEMA));
        elasticsearchClientWrapper.createMapping(INDEX, TYPE, Schema.STRING_SCHEMA);
        verify(indicesClient).putMapping(any(PutMappingRequest.class));
    }

    @Test(expected = ConnectException.class)
    public void createsMappingAndFails() throws Exception {
        final ElasticsearchIndicesClient indicesClient = mock(ElasticsearchIndicesClient.class);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        when(indicesClient.putMapping(any(PutMappingRequest.class)))
            .thenThrow(
                new ElasticsearchException("endpointId", new ErrorResponse.Builder()
                    .error(new ErrorCause.Builder()
                        .reason("failure")
                        .build())
                    .status(500)
                    .build())
            );
        elasticsearchClientWrapper.createMapping(INDEX, TYPE, Schema.STRING_SCHEMA);
    }

    @Test
    public void getsMapping() throws Exception {
        final ElasticsearchIndicesClient indicesClient = mock(ElasticsearchIndicesClient.class);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        final IndexMappingRecord indexMappingRecord = new IndexMappingRecord.Builder()
            .mappings(new TypeMapping.Builder()
                .properties(TYPE, builder -> {
                    return builder.text(new TextProperty.Builder().build());
                }).build())
            .build();
        final Map<String, IndexMappingRecord> indexMappingRecordMap = new HashMap<>();
        indexMappingRecordMap.put(INDEX, indexMappingRecord);
        when(indicesClient.getMapping(any(GetMappingRequest.class))).thenReturn(
            new GetMappingResponse.Builder()
                .result(indexMappingRecordMap)
                .build()
        );

        assertEquals(
            new Property.Builder().text(new TextProperty.Builder().build()).build()._kind().jsonValue(),
            elasticsearchClientWrapper.getMapping(INDEX, TYPE)._kind().jsonValue()
        );
    }

    @Test
    public void executesBulk() throws Exception {
        final List<BulkResponseItem> items = new ArrayList<>();
        items.add(new BulkResponseItem.Builder()
            .operationType(OperationType.Create)
            .index(INDEX)
            .status(200)
            .build()
        );
        when(elasticsearchClient.bulk(any(co.elastic.clients.elasticsearch.core.BulkRequest.class))).thenReturn(
            new BulkResponse.Builder()
                .items(items)
                .errors(false)
                .took(100)
                .build()
        );

        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = elasticsearchClientWrapper.createBulkRequest(records);
        assertThat(elasticsearchClientWrapper.executeBulk(request).isSucceeded(), is(equalTo(true)));
    }

    @Test
    public void executesBulkAndFails() throws Exception {
        final List<BulkResponseItem> items = new ArrayList<>();
        items.add(new BulkResponseItem.Builder()
            .operationType(OperationType.Create)
            .index(INDEX)
            .status(200)
                .error(builder -> builder
                    .type("failure")
                    .reason("failure")
                )
            .build()
        );
        when(elasticsearchClient.bulk(any(co.elastic.clients.elasticsearch.core.BulkRequest.class))).thenReturn(
            new BulkResponse.Builder()
                .items(items)
                .errors(true)
                .took(100)
                .build()
        );

        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), null, 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = elasticsearchClientWrapper.createBulkRequest(records);
        final io.aiven.connect.elasticsearch.bulk.BulkResponse bulkResponse =
            elasticsearchClientWrapper.executeBulk(request);
        assertThat(bulkResponse.isSucceeded(), is(equalTo(false)));
        assertThat(bulkResponse.isRetriable(), is(equalTo(true)));
        assertEquals("[failure]", bulkResponse.getErrorInfo());
    }

    @Test
    public void executesBulkAndFailsWithParseError() throws Exception {
        final List<BulkResponseItem> items = new ArrayList<>();
        items.add(new BulkResponseItem.Builder()
            .operationType(OperationType.Create)
            .index(INDEX)
            .status(200)
            .error(builder -> builder
                .type("mapper_parse_exception")
                .reason("[type=mapper_parse_exception, reason=[key]: Mapper parse error]")
            )
            .build()
        );
        when(elasticsearchClient.bulk(any(co.elastic.clients.elasticsearch.core.BulkRequest.class))).thenReturn(
            new BulkResponse.Builder()
                .items(items)
                .errors(true)
                .took(100)
                .build()
        );

        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = elasticsearchClientWrapper.createBulkRequest(records);
        final io.aiven.connect.elasticsearch.bulk.BulkResponse bulkResponse =
            elasticsearchClientWrapper.executeBulk(request);
        assertThat(bulkResponse.isSucceeded(), is(equalTo(false)));
        assertThat(bulkResponse.isRetriable(), is(equalTo(false)));
        assertEquals(
            "[[type=mapper_parse_exception, reason=[key]: Mapper parse error]]",
            bulkResponse.getErrorInfo()
        );
    }

    @Test
    public void executesBulkAndFailsWithSomeOtherError() throws Exception {
        final List<BulkResponseItem> items = new ArrayList<>();
        items.add(new BulkResponseItem.Builder()
            .operationType(OperationType.Create)
            .index(INDEX)
            .status(200)
            .error(builder -> builder
                .type("random_error_type_string")
                .reason("[type=random_type_string, reason=[key]: Unknown error]")
            )
            .build()
        );
        when(elasticsearchClient.bulk(any(co.elastic.clients.elasticsearch.core.BulkRequest.class))).thenReturn(
            new BulkResponse.Builder()
                .items(items)
                .errors(true)
                .took(100)
                .build()
        );

        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = elasticsearchClientWrapper.createBulkRequest(records);
        final io.aiven.connect.elasticsearch.bulk.BulkResponse bulkResponse =
            elasticsearchClientWrapper.executeBulk(request);
        assertThat(bulkResponse.isSucceeded(), is(equalTo(false)));
        assertThat(bulkResponse.isRetriable(), is(equalTo(true)));
        assertEquals(
            "[[type=random_type_string, reason=[key]: Unknown error]]",
            bulkResponse.getErrorInfo()
        );
    }

    @Test
    public void executesBulkAndSucceedsBecauseOnlyVersionConflicts() throws Exception {
        final List<BulkResponseItem> items = new ArrayList<>();
        items.add(new BulkResponseItem.Builder()
            .operationType(OperationType.Create)
            .index(INDEX)
            .status(200)
            .error(builder -> builder
                .type("version_conflict_engine_exception")
                .reason("[type=version_conflict_engine_exception, reason=[key]: "
                    + "version conflict, current version [1] is higher or equal to the one provided [0]]")
            )
            .build()
        );
        when(elasticsearchClient.bulk(any(co.elastic.clients.elasticsearch.core.BulkRequest.class))).thenReturn(
            new BulkResponse.Builder()
                .items(items)
                .errors(true)
                .took(100)
                .build()
        );


        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = elasticsearchClientWrapper.createBulkRequest(records);
        final io.aiven.connect.elasticsearch.bulk.BulkResponse bulkResponse =
            elasticsearchClientWrapper.executeBulk(request);
        assertThat(bulkResponse.isSucceeded(), is(equalTo(true)));
    }

    @Test
    public void searches() throws Exception {
        final SearchResponse<JsonNode> response = new SearchResponse.Builder<JsonNode>()
            .took(100)
            .timedOut(false)
            .hits(
                new HitsMetadata.Builder<JsonNode>()
                    .hits(new ArrayList<>())
                    .build()
            )
            .shards(
                new ShardStatistics.Builder()
                    .failed(1)
                    .successful(1)
                    .total(2)
                    .build()
            ).build();
        when(elasticsearchClient.<JsonNode>search(any(SearchRequest.class), any())).thenReturn(response);
        assertNotNull(elasticsearchClientWrapper.search(INDEX));
        verify(elasticsearchClient).search(any(SearchRequest.class), any());
    }

    @Test
    public void closes() throws IOException {
        elasticsearchClientWrapper.close();
        verify(elasticsearchTransport).close();
    }
}
