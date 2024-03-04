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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import io.aiven.connect.elasticsearch.ElasticsearchClient;
import io.aiven.connect.elasticsearch.ElasticsearchSinkConnectorConfig;
import io.aiven.connect.elasticsearch.IndexableRecord;
import io.aiven.connect.elasticsearch.Key;
import io.aiven.connect.elasticsearch.Mapping;
import io.aiven.connect.elasticsearch.bulk.BulkRequest;
import io.aiven.connect.elasticsearch.bulk.BulkResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RestHighLevelClientBuilder;
import org.elasticsearch.client.core.MainResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.client.indices.GetMappingsResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AivenElasticsearchClientWrapper implements ElasticsearchClient {

    // visible for testing
    protected static final String MAPPER_PARSE_EXCEPTION
        = "mapper_parse_exception";
    protected static final String VERSION_CONFLICT_ENGINE_EXCEPTION
        = "version_conflict_engine_exception";

    private static final Logger LOG = LoggerFactory.getLogger(AivenElasticsearchClientWrapper.class);

    @SuppressWarnings("deprecation")
    private final RestHighLevelClient elasticClient;
    private final Version version;

    // visible for testing
    @SuppressWarnings("deprecation")

    public AivenElasticsearchClientWrapper(final RestHighLevelClient client) {
        try {
            this.elasticClient = client;
            this.version = getServerVersion();
        } catch (final IOException e) {
            throw new ConnectException(
                "Couldn't start ElasticsearchSinkTask due to connection error:",
                e
            );
        }
    }

    // visible for testing
    public AivenElasticsearchClientWrapper(final String address) {
        try {
            final Map<String, String> props = new HashMap<>();
            props.put(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG, address);
            props.put(ElasticsearchSinkConnectorConfig.TYPE_NAME_CONFIG, "kafka-connect");
            this.elasticClient = getElasticsearchClient(new ElasticsearchSinkConnectorConfig(props));
            this.version = getServerVersion();
        } catch (final IOException e) {
            throw new ConnectException(
                "Couldn't start ElasticsearchSinkTask due to connection error:",
                e
            );
        } catch (final ConfigException e) {
            throw new ConnectException(
                "Couldn't start ElasticsearchSinkTask due to configuration error:",
                e
            );
        }
    }

    // visible for testing
    public AivenElasticsearchClientWrapper(final ElasticsearchSinkConnectorConfig config) {
        try {
            this.elasticClient = getElasticsearchClient(config);
            this.version = getServerVersion();
        } catch (final IOException e) {
            throw new ConnectException(
                "Couldn't start ElasticsearchSinkTask due to connection error:",
                e
            );
        } catch (final ConfigException e) {
            throw new ConnectException(
                "Couldn't start ElasticsearchSinkTask due to configuration error:",
                e
            );
        }
    }

    private Version getServerVersion() throws IOException {
        final String esVersion = this.elasticClient.info(RequestOptions.DEFAULT).getVersion().getNumber();
        return matchVersionString(esVersion);
    }

    private Version matchVersionString(final String esVersion) {
        // Default to newest version for forward compatibility
        final Version defaultVersion = Version.ES_V8;
        if (esVersion == null) {
            LOG.warn("Couldn't get Elasticsearch version, version is null");
            return defaultVersion;
        } else if (esVersion.startsWith("1.")) {
            return Version.ES_V1;
        } else if (esVersion.startsWith("2.")) {
            return Version.ES_V2;
        } else if (esVersion.startsWith("5.")) {
            return Version.ES_V5;
        } else if (esVersion.startsWith("6.")) {
            return Version.ES_V6;
        } else if (esVersion.startsWith("7.")) {
            return Version.ES_V7;
        } else if (esVersion.startsWith("8.")) {
            return Version.ES_V8;
        }
        return defaultVersion;
    }

    private boolean es8compat(final Version version) {
        return Objects.requireNonNull(version) == Version.ES_V8;
    }

    @SuppressWarnings("deprecation")
    private RestHighLevelClient getElasticsearchClient(final ElasticsearchSinkConnectorConfig config) {
        final HttpHost[] httpHosts =
            config.getList(ElasticsearchSinkConnectorConfig.CONNECTION_URL_CONFIG).stream().map(HttpHost::create)
                .toArray(HttpHost[]::new);
        final RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);
        final Optional<String> username = Optional.ofNullable(
            config.getString(ElasticsearchSinkConnectorConfig.CONNECTION_USERNAME_CONFIG)
        );
        final Optional<Password> password = Optional.ofNullable(
            config.getPassword(ElasticsearchSinkConnectorConfig.CONNECTION_PASSWORD_CONFIG)
        );
        final CredentialsProvider credentialsProvider;
        if (username.isPresent() && password.isPresent()) {
            credentialsProvider = new BasicCredentialsProvider();
            final AuthScope scope = new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, AuthScope.ANY_REALM);
            final UsernamePasswordCredentials usernamePasswordCredentials =
                new UsernamePasswordCredentials(username.get(), password.get().value());
            credentialsProvider.setCredentials(scope, usernamePasswordCredentials);
        } else {
            credentialsProvider = null;
        }
        restClientBuilder.setHttpClientConfigCallback(httpClientConfigCallback -> {
            httpClientConfigCallback.setDefaultCredentialsProvider(credentialsProvider);
            return httpClientConfigCallback.addInterceptorLast((HttpResponseInterceptor)
                (response, context) ->
                    response.addHeader("X-Elastic-Product", "Elasticsearch"));
        });

        final int connTimeout = config.getInt(ElasticsearchSinkConnectorConfig.CONNECTION_TIMEOUT_MS_CONFIG);
        final int readTimeout = config.getInt(ElasticsearchSinkConnectorConfig.READ_TIMEOUT_MS_CONFIG);
        restClientBuilder.setRequestConfigCallback(requestConfigCallback -> {
            requestConfigCallback.setConnectTimeout(connTimeout);
            requestConfigCallback.setSocketTimeout(readTimeout);
            return requestConfigCallback;
        });

        final RestClient restClient = restClientBuilder.build();
        final Version version = resolveVersion(restClient);
        return new RestHighLevelClientBuilder(restClient)
            .setApiCompatibilityMode(es8compat(version))
            .build();
    }

    @SuppressWarnings("deprecation")
    private Version resolveVersion(final RestClient restClient) {
        // No auto-closing, it would close also the restClient.
        final RestHighLevelClient highLevelClient = new RestHighLevelClientBuilder(restClient).build();
        try {
            final MainResponse.Version version = highLevelClient.info(RequestOptions.DEFAULT).getVersion();
            return matchVersionString(version.getNumber());
        } catch (final IOException e) {
            throw new ConnectException("Failed to get ElasticSearch version.", e);
        }
    }

    public Version getVersion() {
        return version;
    }

    private boolean indexExists(final String index) {
        final GetIndexRequest getIndexRequest = new GetIndexRequest(index);
        try {
            return elasticClient.indices().exists(getIndexRequest, RequestOptions.DEFAULT);
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    public void createIndices(final Set<String> indices) {
        for (final String index : indices) {
            if (!indexExists(index)) {
                final CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
                try {
                    elasticClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                } catch (final IOException e) {
                    throw new ConnectException("Could not create index '" + index + "'", e);
                }
            }
        }
    }

    public void createMapping(final String index, final String type, final Schema schema) throws IOException {
        final ObjectNode obj = JsonNodeFactory.instance.objectNode();
        obj.set(type, Mapping.inferMapping(getVersion(), schema));
        final JsonNode part = Mapping.inferMapping(getVersion(), schema);
        final PutMappingRequest request = new PutMappingRequest(index)
            .source(part.toString(), XContentType.JSON);
        try {
            this.elasticClient.indices().putMapping(request, RequestOptions.DEFAULT);
        } catch (final ElasticsearchException exception) {
            throw new ConnectException(
                "Cannot create mapping " + schema + " -- " + exception.getDetailedMessage()
            );
        }
    }

    /**
     * Get the JSON mapping for given index and type. Returns {@code null} if it does not exist.
     */
    @Override
    public MappingMetadata getMapping(final String index, final String type) throws IOException {
        final GetMappingsRequest request = new GetMappingsRequest()
            .indices(index);
        final GetMappingsResponse response = this.elasticClient.indices().getMapping(request, RequestOptions.DEFAULT);
        if (response.mappings().isEmpty()) {
            return null;
        }
        return response.mappings().get(index);
    }

    public BulkRequest createBulkRequest(final List<IndexableRecord> batch) {
        final org.elasticsearch.action.bulk.BulkRequest bulkRequest = new org.elasticsearch.action.bulk.BulkRequest();
        for (final IndexableRecord record : batch) {
            bulkRequest.add(toBulkableAction(record));
        }
        return new BulkRequestImpl(bulkRequest);
    }

    // visible for testing
    protected DocWriteRequest<?> toBulkableAction(final IndexableRecord record) {
        // If payload is null, the record was a tombstone and we should delete from the index.
        return record.payload != null ? toIndexRequest(record) : toDeleteRequest(record);
    }

    private DeleteRequest toDeleteRequest(final IndexableRecord record) {
        // TODO: Should version information be set here?
        return new DeleteRequest(record.key.index).id(record.key.id);
    }

    private IndexRequest toIndexRequest(final IndexableRecord record) {
        final IndexRequest indexRequest = new IndexRequest(record.key.index)
            .id(record.key.id)
            .source(record.payload, XContentType.JSON);
        if (record.version != null) {
            indexRequest
                .versionType(VersionType.EXTERNAL)
                .version(record.version);
        }
        return indexRequest;
    }

    public BulkResponse executeBulk(final BulkRequest bulk) throws IOException {
        final BulkRequestImpl bulkRequest = (BulkRequestImpl) bulk;
        final org.elasticsearch.action.bulk.BulkResponse
            response = elasticClient.bulk(bulkRequest.getBulkRequest(), RequestOptions.DEFAULT);

        if (!response.hasFailures()) {
            return BulkResponse.success();
        }

        boolean retriable = true;

        final List<Key> versionConflicts = new ArrayList<>();
        final List<String> errors = new ArrayList<>();

        for (final BulkItemResponse item : response.getItems()) {
            if (item.isFailed()) {
                final BulkItemResponse.Failure failure = item.getFailure();
                final String errorType = Optional.ofNullable(failure.getCause().getMessage()).orElse("");
                if (errorType.contains("version_conflict_engine_exception")) {
                    versionConflicts.add(new Key(item.getIndex(), item.getType(), item.getId()));
                } else if (errorType.contains("mapper_parse_exception")) {
                    retriable = false;
                    errors.add(item.getFailureMessage());
                } else {
                    errors.add(item.getFailureMessage());
                }
            }
        }

        if (!versionConflicts.isEmpty()) {
            LOG.debug("Ignoring version conflicts for items: {}", versionConflicts);
            if (errors.isEmpty()) {
                // The only errors were version conflicts
                return BulkResponse.success();
            }
        }

        final String errorInfo = errors.isEmpty() ? response.buildFailureMessage() : errors.toString();

        return BulkResponse.failure(retriable, errorInfo);
    }

    @Override
    public SearchResponse search(final String index) throws IOException {
        final SearchRequest searchRequest = new SearchRequest();
        if (index != null) {
            searchRequest.indices(index);
        }
        return elasticClient.search(searchRequest, RequestOptions.DEFAULT);
    }

    public void refresh(final String index) throws IOException {
        final RefreshRequest request = new RefreshRequest(index);
        elasticClient.indices().refresh(request, RequestOptions.DEFAULT);
    }

    public void close() throws IOException {
        elasticClient.close();
    }
}
