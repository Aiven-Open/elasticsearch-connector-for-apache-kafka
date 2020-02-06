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

package io.aiven.connect.elasticsearch.jest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.elasticsearch.ElasticsearchClient;
import io.aiven.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.aiven.connect.elasticsearch.IndexableRecord;
import io.aiven.connect.elasticsearch.Key;
import io.aiven.connect.elasticsearch.Mapping;
import io.aiven.connect.elasticsearch.bulk.BulkRequest;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.ElasticsearchVersion;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.NodesInfo;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JestElasticsearchClientTest {

    private static final String INDEX = "index";
    private static final String BAD_INDEX = "bad_bad_index";
    private static final String KEY = "key";
    private static final String TYPE = "type";
    private static final String QUERY = "query";

    private JestClient jestClient;
    private JestClientFactory jestClientFactory;
    private NodesInfo info;

    @Before
    public void setUp() throws Exception {
        jestClient = mock(JestClient.class);
        jestClientFactory = mock(JestClientFactory.class);
        when(jestClientFactory.getObject()).thenReturn(jestClient);
        info = new NodesInfo.Builder().addCleanApiParameter("version").build();
        final JsonObject nodeRoot = new JsonObject();
        nodeRoot.addProperty("version", "1.0");
        final JsonObject nodesRoot = new JsonObject();
        nodesRoot.add("localhost", nodeRoot);
        final JsonObject nodesInfo = new JsonObject();
        nodesInfo.add("nodes", nodesRoot);
        final JestResult result = new JestResult(new Gson());
        result.setJsonObject(nodesInfo);
        when(jestClient.execute(info)).thenReturn(result);
    }

    @Test
    public void connectsSecurely() {
        final Map<String, String> props = new HashMap<>();
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost:9200");
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG, "elastic");
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG, "elasticpw");
        props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, "kafka-connect");
        final JestElasticsearchClient client = new JestElasticsearchClient(props, jestClientFactory);

        final ArgumentCaptor<HttpClientConfig> captor = ArgumentCaptor.forClass(HttpClientConfig.class);
        verify(jestClientFactory).setHttpClientConfig(captor.capture());
        final HttpClientConfig httpClientConfig = captor.getValue();
        final CredentialsProvider credentialsProvider = httpClientConfig.getCredentialsProvider();
        final Credentials credentials = credentialsProvider.getCredentials(AuthScope.ANY);
        final Set<HttpHost> preemptiveAuthTargetHosts = httpClientConfig.getPreemptiveAuthTargetHosts();
        assertEquals("elastic", credentials.getUserPrincipal().getName());
        assertEquals("elasticpw", credentials.getPassword());
        assertEquals(HttpHost.create("http://localhost:9200"), preemptiveAuthTargetHosts.iterator().next());
    }

    @Test
    public void connectsSecurelyWithEmptyUsernameAndPassword() {
        final Map<String, String> props = new HashMap<>();
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, "http://localhost:9200");
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG, "");
        props.put(ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG, "");
        props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, "kafka-connect");
        final JestElasticsearchClient client = new JestElasticsearchClient(props, jestClientFactory);

        final ArgumentCaptor<HttpClientConfig> captor = ArgumentCaptor.forClass(HttpClientConfig.class);
        verify(jestClientFactory).setHttpClientConfig(captor.capture());
        final HttpClientConfig httpClientConfig = captor.getValue();
        final CredentialsProvider credentialsProvider = httpClientConfig.getCredentialsProvider();
        final Credentials credentials = credentialsProvider.getCredentials(AuthScope.ANY);
        final Set<HttpHost> preemptiveAuthTargetHosts = httpClientConfig.getPreemptiveAuthTargetHosts();
        assertEquals("", credentials.getUserPrincipal().getName());
        assertEquals("", credentials.getPassword());
        assertEquals(HttpHost.create("http://localhost:9200"), preemptiveAuthTargetHosts.iterator().next());
    }

    @Test
    public void getsVersion() {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        assertThat(client.getVersion(), is(equalTo(ElasticsearchClient.Version.ES_V1)));
    }

    @Test
    public void createsIndices() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final JestResult failure = new JestResult(new Gson());
        failure.setSucceeded(false);
        final JestResult success = new JestResult(new Gson());
        success.setSucceeded(true);
        final IndicesExists indicesExists = new IndicesExists.Builder(INDEX).build();
        when(jestClient.execute(indicesExists)).thenReturn(failure);
        when(jestClient.execute(argThat(isCreateIndexForTestIndex()))).thenReturn(success);

        final Set<String> indices = Sets.newHashSet();
        indices.add(INDEX);
        client.createIndices(indices);
        final InOrder inOrder = inOrder(jestClient);
        inOrder.verify(jestClient).execute(info);
        inOrder.verify(jestClient).execute(indicesExists);
        inOrder.verify(jestClient).execute(argThat(isCreateIndexForTestIndex()));
    }

    private ArgumentMatcher<CreateIndex> isCreateIndexForTestIndex() {
        return new ArgumentMatcher<CreateIndex>() {
            @Override
            public boolean matches(final CreateIndex createIndex) {
                // check the URI as the equals method on CreateIndex doesn't work
                return createIndex.getURI(ElasticsearchVersion.V2).equals(INDEX);
            }
        };
    }

    @Test(expected = ConnectException.class)
    public void createIndicesAndFails() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final JestResult failure = new JestResult(new Gson());
        failure.setSucceeded(false);
        final IndicesExists indicesExists = new IndicesExists.Builder(INDEX).build();
        when(jestClient.execute(indicesExists)).thenReturn(failure);
        when(jestClient.execute(argThat(isCreateIndexForTestIndex()))).thenReturn(failure);

        final Set<String> indices = new HashSet<>();
        indices.add(INDEX);
        client.createIndices(indices);
    }

    @Test
    public void createsMapping() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final JestResult success = new JestResult(new Gson());
        success.setSucceeded(true);
        final ObjectNode obj = JsonNodeFactory.instance.objectNode();
        obj.set(TYPE, Mapping.inferMapping(client, Schema.STRING_SCHEMA));
        final PutMapping putMapping = new PutMapping.Builder(INDEX, TYPE, obj.toString()).build();
        when(jestClient.execute(putMapping)).thenReturn(success);

        client.createMapping(INDEX, TYPE, Schema.STRING_SCHEMA);
        verify(jestClient).execute(putMapping);
    }

    @Test(expected = ConnectException.class)
    public void createsMappingAndFails() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final JestResult failure = new JestResult(new Gson());
        failure.setSucceeded(false);
        final ObjectNode obj = JsonNodeFactory.instance.objectNode();
        obj.set(TYPE, Mapping.inferMapping(client, Schema.STRING_SCHEMA));
        final PutMapping putMapping = new PutMapping.Builder(INDEX, TYPE, obj.toString()).build();
        when(jestClient.execute(putMapping)).thenReturn(failure);

        client.createMapping(INDEX, TYPE, Schema.STRING_SCHEMA);
    }

    @Test
    public void getsMapping() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final JsonObject mapping = new JsonObject();
        final JsonObject mappings = new JsonObject();
        mappings.add(TYPE, mapping);
        final JsonObject indexRoot = new JsonObject();
        indexRoot.add("mappings", mappings);
        final JsonObject root = new JsonObject();
        root.add(INDEX, indexRoot);
        final JestResult result = new JestResult(new Gson());
        result.setJsonObject(root);
        final GetMapping getMapping = new GetMapping.Builder().addIndex(INDEX).addType(TYPE).build();
        when(jestClient.execute(getMapping)).thenReturn(result);

        assertThat(client.getMapping(INDEX, TYPE), is(equalTo(mapping)));
    }

    @Test
    public void executesBulk() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = client.createBulkRequest(records);
        final BulkResult success = new BulkResult(new Gson());
        success.setSucceeded(true);
        when(jestClient.execute(((JestBulkRequest) request).getBulk())).thenReturn(success);

        assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(true)));
    }

    @Test
    public void executesBulkAndFails() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), null, 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = client.createBulkRequest(records);
        final BulkResult failure = new BulkResult(new Gson());
        failure.setSucceeded(false);
        when(jestClient.execute(((JestBulkRequest) request).getBulk())).thenReturn(failure);

        assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(false)));
    }

    @Test
    public void executesBulkAndFailsWithParseError() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = client.createBulkRequest(records);
        final BulkResult failure = createBulkResultFailure(JestElasticsearchClient.MAPPER_PARSE_EXCEPTION);
        when(jestClient.execute(((JestBulkRequest) request).getBulk())).thenReturn(failure);

        assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(false)));
    }

    @Test
    public void executesBulkAndFailsWithSomeOtherError() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = client.createBulkRequest(records);
        final BulkResult failure = createBulkResultFailure("some_random_exception");
        when(jestClient.execute(((JestBulkRequest) request).getBulk())).thenReturn(failure);

        assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(false)));
    }

    @Test
    public void executesBulkAndSucceedsBecauseOnlyVersionConflicts() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final IndexableRecord record = new IndexableRecord(new Key(INDEX, TYPE, KEY), "payload", 0L);
        final List<IndexableRecord> records = new ArrayList<>();
        records.add(record);
        final BulkRequest request = client.createBulkRequest(records);
        final BulkResult failure = createBulkResultFailure(JestElasticsearchClient.VERSION_CONFLICT_ENGINE_EXCEPTION);
        when(jestClient.execute(((JestBulkRequest) request).getBulk())).thenReturn(failure);

        assertThat(client.executeBulk(request).isSucceeded(), is(equalTo(true)));
    }

    @Test
    public void searches() throws Exception {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        final Search search = new Search.Builder(QUERY).addIndex(INDEX).addType(TYPE).build();
        final JsonObject queryResult = new JsonObject();
        final SearchResult result = new SearchResult(new Gson());
        result.setJsonObject(queryResult);
        when(jestClient.execute(search)).thenReturn(result);

        assertThat(client.search(QUERY, INDEX, TYPE), is(equalTo(queryResult)));
    }

    @Test
    public void closes() throws IOException {
        final JestElasticsearchClient client = new JestElasticsearchClient(jestClient);
        client.close();

        verify(jestClient).close();
    }

    private BulkResult createBulkResultFailure(final String exception) {
        final BulkResult failure = new BulkResult(new Gson());
        failure.setSucceeded(false);
        final JsonObject error = new JsonObject();
        error.addProperty("type", exception);
        final JsonObject item = new JsonObject();
        item.addProperty("_index", INDEX);
        item.addProperty("_type", TYPE);
        item.addProperty("status", 0);
        item.add("error", error);
        final JsonObject index = new JsonObject();
        index.add("index", item);
        final JsonArray items = new JsonArray();
        items.add(index);
        final JsonObject root = new JsonObject();
        root.add("items", items);
        failure.setJsonObject(root);
        return failure;
    }
}
