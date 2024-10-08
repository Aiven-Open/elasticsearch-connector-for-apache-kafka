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

package io.aiven.connect.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;

import io.aiven.connect.elasticsearch.bulk.BulkRequest;
import io.aiven.connect.elasticsearch.bulk.BulkResponse;

import co.elastic.clients.elasticsearch._types.mapping.Property;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.fasterxml.jackson.databind.JsonNode;

public interface ElasticsearchClient extends AutoCloseable {

    enum Version {
        ES_V1, ES_V2, ES_V5, ES_V6, ES_V7, ES_V8
    }

    /**
     * Gets the Elasticsearch version.
     *
     * @return the version, not null
     */
    Version getVersion();

    /**
     * Creates indices.
     *
     * @param indices the set of index names to create, not null
     */
    void createIndices(Set<String> indices);

    /**
     * Creates an explicit mapping.
     *
     * @param index  the index to write
     * @param type   the type for which to create the mapping
     * @param schema the schema used to infer the mapping
     * @throws IOException if the client cannot execute the request
     */
    void createMapping(String index, String type, Schema schema) throws IOException;

    /**
     * Gets the JSON mapping for the given index and type. Returns {@code null} if it does not exist.
     *
     * @param index the index
     * @param type  the type
     * @throws IOException if the client cannot execute the request
     */
    Property getMapping(String index, String type) throws IOException;

    /**
     * Creates a bulk request for the list of {@link IndexableRecord} records.
     *
     * @param batch the list of records
     * @return the bulk request
     */
    BulkRequest createBulkRequest(List<IndexableRecord> batch);

    /**
     * Executes a bulk action.
     *
     * @param bulk the bulk request
     * @return the bulk response
     * @throws IOException if the client cannot execute the request
     */
    BulkResponse executeBulk(BulkRequest bulk) throws IOException;

    /**
     * Executes a search.
     *
     * @param index the index to search
     * @return the search result
     * @throws IOException if the client cannot execute the request
     */
    SearchResponse<JsonNode> search(String index) throws IOException;

    /**
     * Refreshes the index.
     *
     * @param index the index to refresh
     */
    void refreshIndex(String index) throws IOException;

    /**
     * Shuts down the client.
     */
    void close() throws IOException;
}
