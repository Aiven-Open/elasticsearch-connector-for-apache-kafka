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

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHits;
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

public class AivenElasticsearchClientWrapperTest {

    private static final String INDEX = "index";
    private static final String KEY = "key";
    private static final String TYPE = "type";

    private RestHighLevelClient elasticsearchClient;

    @Before
    public void setUp() throws Exception {
        elasticsearchClient = mock(RestHighLevelClient.class);
        when(elasticsearchClient.info(any())).thenReturn(
            new MainResponse("localhost",
                new MainResponse.Version("1.0", "buildFlavor",
                    "buildType", "buildHash", "buildDate", false,
                    "luceneVersion",
                    "minWireCompVersion", "minIndexCompVersion"),
                "clusterName", UUID.randomUUID().toString(), "tagLine"
            )
        );
    }

    @Test
    public void getsVersion() {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        assertThat(client.getVersion(), is(equalTo(ElasticsearchClient.Version.ES_V1)));
    }

    @Test
    public void createsIndices() throws Exception {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final IndicesClient indicesClient = mock(IndicesClient.class);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        when(indicesClient.exists(any(GetIndexRequest.class), any())).thenReturn(false);
        when(indicesClient.create(any(CreateIndexRequest.class), any())).thenThrow(new IOException("failure"));

        when(indicesClient.exists(any(GetIndexRequest.class), any())).thenReturn(false);
        when(indicesClient.create(argThat(isCreateIndexForTestIndex()), any())).thenReturn(
            new CreateIndexResponse(true, true, INDEX));

        final Set<String> indices = Sets.newHashSet();
        indices.add(INDEX);
        client.createIndices(indices);
        final InOrder inOrder = inOrder(elasticsearchClient, indicesClient);
        inOrder.verify(elasticsearchClient).info(any());
        inOrder.verify(indicesClient).exists(any(GetIndexRequest.class), any());
        inOrder.verify(indicesClient).create(argThat(isCreateIndexForTestIndex()), any());
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
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final IndicesClient indicesClient = mock(IndicesClient.class);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        when(indicesClient.exists(any(GetIndexRequest.class), any())).thenReturn(false);
        when(indicesClient.create(any(CreateIndexRequest.class), any())).thenThrow(new IOException("failure"));

        final Set<String> indices = new HashSet<>();
        indices.add("test-index");
        assertThrows("Could not create index 'test-index'", ConnectException.class, () -> {
            client.createIndices(indices);
        });
    }

    @Test
    public void createsMapping() throws Exception {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final IndicesClient indicesClient = mock(IndicesClient.class);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        final ObjectNode obj = JsonNodeFactory.instance.objectNode();
        obj.set(TYPE, Mapping.inferMapping(client.getVersion(), Schema.STRING_SCHEMA));
        client.createMapping(INDEX, TYPE, Schema.STRING_SCHEMA);
        verify(indicesClient).putMapping(any(PutMappingRequest.class), any());
    }

    @Test(expected = ConnectException.class)
    public void createsMappingAndFails() throws Exception {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final IndicesClient indicesClient = mock(IndicesClient.class);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        when(indicesClient.putMapping(any(PutMappingRequest.class), any()))
            .thenThrow(new ElasticsearchException("failure"));
        client.createMapping(INDEX, TYPE, Schema.STRING_SCHEMA);
    }

    @Test
    public void getsMapping() throws Exception {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final IndicesClient indicesClient = mock(IndicesClient.class);
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
        final MappingMetadata mappingMetadata = new MappingMetadata(TYPE, new HashMap<>());
        final Map<String, MappingMetadata> mappingsMetadata = new HashMap<>();
        mappingsMetadata.put(INDEX, mappingMetadata);
        when(indicesClient.getMapping(any(GetMappingsRequest.class), any())).thenReturn(
            new GetMappingsResponse(mappingsMetadata));
        assertEquals(client.getMapping(INDEX, TYPE), new MappingMetadata(TYPE, new HashMap<>()));
    }

    @Test
    public void executesBulk() throws Exception {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            BulkItemResponse.success(1, DocWriteRequest.OpType.CREATE,
                new IndexResponse(ShardId.fromString("[" + INDEX + "][1]"), TYPE, "id", 1L, 1L, 1L, true)
            )
        };
        when(elasticsearchClient.bulk(any(), any())).thenReturn(
            new BulkResponse(bulkItemResponses, 100)
        );

        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = client.createBulkRequest(records);
        assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(true)));
    }

    @Test
    public void executesBulkAndFails() throws Exception {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            BulkItemResponse.failure(1, DocWriteRequest.OpType.CREATE, new BulkItemResponse.Failure(
                INDEX, TYPE, "id", new IOException("failure")
            ))
        };
        when(elasticsearchClient.bulk(any(), any())).thenReturn(
            new BulkResponse(bulkItemResponses, 100)
        );

        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), null, 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = client.createBulkRequest(records);
        final io.aiven.connect.elasticsearch.bulk.BulkResponse bulkResponse = client.executeBulk(request);
        assertThat(bulkResponse.isSucceeded(), is(equalTo(false)));
        assertThat(bulkResponse.isRetriable(), is(equalTo(true)));
        assertEquals("[java.io.IOException: failure]", bulkResponse.getErrorInfo());
    }

    @Test
    public void executesBulkAndFailsWithParseError() throws Exception {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            BulkItemResponse.failure(1, DocWriteRequest.OpType.CREATE, new BulkItemResponse.Failure(
                INDEX, AivenElasticsearchClientWrapper.MAPPER_PARSE_EXCEPTION, "id",
                new ElasticsearchException("[type=mapper_parse_exception, reason=[key]: Mapper parse error]")

            ))
        };
        when(elasticsearchClient.bulk(any(), any())).thenReturn(
            new BulkResponse(bulkItemResponses, 100)
        );

        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = client.createBulkRequest(records);
        final io.aiven.connect.elasticsearch.bulk.BulkResponse bulkResponse = client.executeBulk(request);
        assertThat(bulkResponse.isSucceeded(), is(equalTo(false)));
        assertThat(bulkResponse.isRetriable(), is(equalTo(false)));
        assertEquals(
            "[ElasticsearchException[[type=mapper_parse_exception, reason=[key]: Mapper parse error]]]",
            bulkResponse.getErrorInfo()
        );
    }

    @Test
    public void executesBulkAndFailsWithSomeOtherError() throws Exception {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            BulkItemResponse.failure(1, DocWriteRequest.OpType.CREATE, new BulkItemResponse.Failure(
                INDEX, "some_random_exception", "id",
                new ElasticsearchException("[type=random_type_string, reason=[key]: Unknown error]")
            ))
        };
        when(elasticsearchClient.bulk(any(), any())).thenReturn(
            new BulkResponse(bulkItemResponses, 100)
        );

        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = client.createBulkRequest(records);
        final io.aiven.connect.elasticsearch.bulk.BulkResponse bulkResponse = client.executeBulk(request);
        assertThat(bulkResponse.isSucceeded(), is(equalTo(false)));
        assertThat(bulkResponse.isRetriable(), is(equalTo(true)));
        assertEquals(
            "[ElasticsearchException[[type=random_type_string, reason=[key]: Unknown error]]]",
            bulkResponse.getErrorInfo()
        );
    }

    @Test
    public void executesBulkAndSucceedsBecauseOnlyVersionConflicts() throws Exception {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final BulkItemResponse[] bulkItemResponses = new BulkItemResponse[] {
            BulkItemResponse.failure(1, DocWriteRequest.OpType.CREATE, new BulkItemResponse.Failure(
                INDEX, "_doc", "id",
                new ElasticsearchException(
                    "[type=version_conflict_engine_exception, reason=[key]: "
                        + "version conflict, current version [1] is higher or equal to the one provided [0]]")
            ))
        };
        when(elasticsearchClient.bulk(any(), any())).thenReturn(
            new BulkResponse(bulkItemResponses, 100)
        );


        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = client.createBulkRequest(records);
        final io.aiven.connect.elasticsearch.bulk.BulkResponse bulkResponse = client.executeBulk(request);
        assertThat(bulkResponse.isSucceeded(), is(equalTo(true)));
    }

    @Test
    public void searches() throws Exception {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        final SearchResponse response = new SearchResponse(
            new SearchResponseSections(SearchHits.empty(), null, null, false, false, null, 0),
            "scrollId", 1, 1, 0, 100L, null, SearchResponse.Clusters.EMPTY
        );
        when(elasticsearchClient.search(any(SearchRequest.class), any())).thenReturn(response);
        assertNotNull(client.search(INDEX));
        verify(elasticsearchClient).search(any(SearchRequest.class), any());
    }

    @Test
    public void closes() throws IOException {
        final AivenElasticsearchClientWrapper client = new AivenElasticsearchClientWrapper(elasticsearchClient);
        client.close();
        verify(elasticsearchClient).close();
    }
}
