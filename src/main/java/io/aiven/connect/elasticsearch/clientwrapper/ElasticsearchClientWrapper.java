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
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.VersionType;
import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.GetMappingRequest;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.elasticsearch.indices.get_mapping.IndexMappingRecord;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.util.BinaryData;
import co.elastic.clients.util.ContentType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchClientWrapper implements ElasticsearchClient {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchClientWrapper.class);

    private final ElasticsearchTransport elasticTransport;
    private final co.elastic.clients.elasticsearch.ElasticsearchClient elasticClient;
    private final Version version;

    // visible for testing
    public ElasticsearchClientWrapper(
        final ElasticsearchTransport elasticTransport,
        final co.elastic.clients.elasticsearch.ElasticsearchClient elasticClient) {
        Objects.requireNonNull(elasticTransport);
        Objects.requireNonNull(elasticClient);
        try {
            this.elasticTransport = elasticTransport;
            this.elasticClient = elasticClient;
            this.version = getServerVersion();
        } catch (final IOException e) {
            throw new ConnectException(
                "Couldn't start ElasticsearchSinkTask due to connection error:",
                e
            );
        }
    }

    // visible for testing
    public ElasticsearchClientWrapper(final ElasticsearchSinkConnectorConfig config) {
        try {
            this.elasticTransport = getElasticsearchTransport(config);
            this.elasticClient = getElasticsearchClient();
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
        final String esVersion = this.elasticClient.info().version().number();
        return matchVersionString(esVersion);
    }

    private Version matchVersionString(final String esVersion) {
        // Default to latest version for forward compatibility
        final Version defaultVersion = Version.ES_V8;
        if (esVersion == null) {
            LOG.warn("Couldn't get Elasticsearch version, version is null");
            return defaultVersion;
        } else if (esVersion.startsWith("7.")) {
            return Version.ES_V7;
        } else if (esVersion.startsWith("8.")) {
            return Version.ES_V8;
        } else {
            LOG.warn(
                "The Elasticsearch version {} isn't explicitly supported, using the default version {}",
                esVersion, defaultVersion
            );
        }
        return defaultVersion;
    }

    private ElasticsearchTransport getElasticsearchTransport(final ElasticsearchSinkConnectorConfig config) {
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
        return new RestClientTransport(
            restClient,
            new JacksonJsonpMapper()
        );
    }

    private co.elastic.clients.elasticsearch.ElasticsearchClient getElasticsearchClient() {
        return new co.elastic.clients.elasticsearch.ElasticsearchClient(this.elasticTransport);
    }

    public Version getVersion() {
        return version;
    }

    private boolean indexExists(final String index) {
        final ExistsRequest existsRequest = new ExistsRequest.Builder()
            .index(index).build();
        try {
            return elasticClient.indices().exists(existsRequest).value();
        } catch (final IOException e) {
            throw new ConnectException(e);
        }
    }

    public void createIndices(final Set<String> indices) {
        for (final String index : indices) {
            if (!indexExists(index)) {
                final CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
                    .index(index).build();
                try {
                    elasticClient.indices().create(createIndexRequest);
                } catch (final IOException e) {
                    throw new ConnectException("Could not create index '" + index + "'", e);
                }
            }
        }
    }

    public void createMapping(final String index, final String type, final Schema schema) throws IOException {
        final ObjectNode root = JsonNodeFactory.instance.objectNode();
        final JsonNode mapping = Mapping.inferMapping(getVersion(), schema);
        final ObjectNode typeNode = JsonNodeFactory.instance.objectNode();
        typeNode.set(type, mapping);
        root.set("properties", typeNode);
        final PutMappingRequest request = new PutMappingRequest.Builder()
            .index(index)
            .withJson(new StringReader(root.toString()))
            .build();

        try {
            this.elasticClient.indices().putMapping(request);
        } catch (final ElasticsearchException exception) {
            throw new ConnectException(
                "Cannot create mapping " + schema + " -- " + exception.getMessage()
            );
        }
    }

    /**
     * Get the mapping for given index and type. Returns {@code null} if it does not exist.
     */
    @Override
    public Property getMapping(final String index, final String type) throws IOException {
        final GetMappingRequest request = new GetMappingRequest.Builder()
            .index(index).build();
        final GetMappingResponse response = this.elasticClient.indices().getMapping(request);
        final IndexMappingRecord indexMappingRecord = response.get(index);
        if (indexMappingRecord == null) {
            return null;
        }
        return indexMappingRecord.mappings().properties().get(type);
    }

    public BulkRequest createBulkRequest(final List<IndexableRecord> batch) {
        final List<BulkOperation> bulkOperations = new ArrayList<>();
        for (final IndexableRecord record : batch) {
            bulkOperations.add(toBulkableOperation(record));
        }
        final co.elastic.clients.elasticsearch.core.BulkRequest bulkRequest =
            new co.elastic.clients.elasticsearch.core.BulkRequest.Builder()
                .operations(bulkOperations)
                .build();
        return new ElasticsearchBulkRequest(bulkRequest);
    }

    // visible for testing
    protected BulkOperation toBulkableOperation(final IndexableRecord record) {
        // If payload is null, the record was a tombstone and we should delete from the index.
        return record.payload != null ? toIndexOperation(record) : toDeleteOperation(record);
    }

    private BulkOperation toDeleteOperation(final IndexableRecord record) {
        // TODO: Should version information be set here?
        return new BulkOperation.Builder().delete(operation -> operation
            .index(record.key.index)
            .id(record.key.id)
        ).build();
    }

    private BulkOperation toIndexOperation(final IndexableRecord record) {
        final BinaryData binaryPayload =
            BinaryData.of(record.payload.getBytes(StandardCharsets.UTF_8), ContentType.APPLICATION_JSON);
        return new BulkOperation.Builder().index(operation -> {
            operation
                .index(record.key.index)
                .id(record.key.id)
                .document(binaryPayload);
            if (record.version != null) {
                operation
                    .versionType(VersionType.External)
                    .version(record.version);
            }
            return operation;
        }).build();
    }

    public BulkResponse executeBulk(final BulkRequest bulk) throws IOException {
        final co.elastic.clients.elasticsearch.core.BulkResponse response =
            elasticClient.bulk(((ElasticsearchBulkRequest) bulk).getBulkRequest());

        if (!response.errors()) {
            return BulkResponse.success();
        }

        boolean retriable = true;

        final List<Key> versionConflicts = new ArrayList<>();
        final List<String> errors = new ArrayList<>();

        for (final BulkResponseItem item : response.items()) {
            if (item.error() != null) {
                final String errorType = item.error().type();
                if ("version_conflict_engine_exception".equals(errorType)) {
                    versionConflicts.add(new Key(item.index(), item.operationType().name(), item.id()));
                } else if ("mapper_parse_exception".equals(errorType)) {
                    retriable = false;
                    errors.add(item.error().reason());
                } else {
                    errors.add(item.error().reason());
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

        final String errorInfo = errors.isEmpty()
            ? "Errors present, but error information missing."
            : errors.toString();
        LOG.trace("Bulk response: {}", response);

        return BulkResponse.failure(retriable, errorInfo);
    }

    // visible for testing
    @Override
    public SearchResponse<JsonNode> search(final String index) throws IOException {
        final SearchRequest.Builder searchRequestBuilder = new SearchRequest.Builder();

        if (index != null) {
            searchRequestBuilder.index(index);
        }
        return elasticClient.search(searchRequestBuilder.build(), JsonNode.class);
    }

    public void refreshIndex(final String index) throws IOException {
        final RefreshRequest request = new RefreshRequest.Builder()
            .index(index)
            .build();
        elasticClient.indices().refresh(request);
    }

    public void close() throws IOException {
        elasticTransport.close();
    }
}
