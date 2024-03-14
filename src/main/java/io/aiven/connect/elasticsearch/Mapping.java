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
import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;

import co.elastic.clients.elasticsearch._types.mapping.Property;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class Mapping {

    /**
     * Create an explicit mapping.
     *
     * @param client The client to connect to Elasticsearch.
     * @param index  The index to write to Elasticsearch.
     * @param type   The type to create mapping for.
     * @param schema The schema used to infer mapping.
     * @throws IOException from underlying client
     */
    public static void createMapping(
        final ElasticsearchClient client,
        final String index,
        final String type,
        final Schema schema
    ) throws IOException {
        client.createMapping(index, type, schema);
    }

    /**
     * Get the JSON mapping for given index and type. Returns {@code null} if it does not exist.
     */
    public static Property getMapping(final ElasticsearchClient client, final String index, final String type)
        throws IOException {
        return client.getMapping(index, type);
    }

    /**
     * Infer mapping from the provided schema.
     *
     * @param schema The schema used to infer mapping.
     */
    public static JsonNode inferMapping(final ElasticsearchClient.Version version, final Schema schema) {
        if (schema == null) {
            throw new DataException("Cannot infer mapping without schema.");
        }

        // Handle logical types
        final JsonNode logicalConversion = inferLogicalMapping(schema);
        if (logicalConversion != null) {
            return logicalConversion;
        }

        final Schema.Type schemaType = schema.type();
        final ObjectNode properties = JsonNodeFactory.instance.objectNode();
        final ObjectNode fields = JsonNodeFactory.instance.objectNode();
        switch (schemaType) {
            case ARRAY:
                return inferMapping(version, schema.valueSchema());
            case MAP:
                properties.set("properties", fields);
                fields.set(ElasticsearchSinkConnectorConstants.MAP_KEY, inferMapping(version, schema.keySchema()));
                fields.set(ElasticsearchSinkConnectorConstants.MAP_VALUE, inferMapping(version, schema.valueSchema()));
                return properties;
            case STRUCT:
                properties.set("properties", fields);
                for (final Field field : schema.fields()) {
                    fields.set(field.name(), inferMapping(version, field.schema()));
                }
                return properties;
            default:
                final String esType = getElasticsearchType(version, schemaType);
                return inferPrimitive(esType, schema.defaultValue());
        }
    }

    // visible for testing
    protected static String getElasticsearchType(final ElasticsearchClient.Version version,
                                                 final Schema.Type schemaType) {
        switch (schemaType) {
            case BOOLEAN:
                return ElasticsearchSinkConnectorConstants.BOOLEAN_TYPE;
            case INT8:
                return ElasticsearchSinkConnectorConstants.BYTE_TYPE;
            case INT16:
                return ElasticsearchSinkConnectorConstants.SHORT_TYPE;
            case INT32:
                return ElasticsearchSinkConnectorConstants.INTEGER_TYPE;
            case INT64:
                return ElasticsearchSinkConnectorConstants.LONG_TYPE;
            case FLOAT32:
                return ElasticsearchSinkConnectorConstants.FLOAT_TYPE;
            case FLOAT64:
                return ElasticsearchSinkConnectorConstants.DOUBLE_TYPE;
            case STRING:
                switch (version) {
                    case ES_V1:
                    case ES_V2:
                        return ElasticsearchSinkConnectorConstants.STRING_TYPE;
                    case ES_V5:
                    case ES_V6:
                    default:
                        return ElasticsearchSinkConnectorConstants.TEXT_TYPE;
                }
            case BYTES:
                return ElasticsearchSinkConnectorConstants.BINARY_TYPE;
            default:
                return null;
        }
    }

    private static JsonNode inferLogicalMapping(final Schema schema) {
        final String schemaName = schema.name();
        final Object defaultValue = schema.defaultValue();
        if (schemaName == null) {
            return null;
        }

        switch (schemaName) {
            case Date.LOGICAL_NAME:
            case Time.LOGICAL_NAME:
            case Timestamp.LOGICAL_NAME:
                return inferPrimitive(ElasticsearchSinkConnectorConstants.DATE_TYPE, defaultValue);
            case Decimal.LOGICAL_NAME:
                return inferPrimitive(ElasticsearchSinkConnectorConstants.DOUBLE_TYPE, defaultValue);
            default:
                // User-defined type or unknown built-in
                return null;
        }
    }

    private static JsonNode inferPrimitive(final String type, final Object defaultValue) {
        if (type == null) {
            throw new ConnectException("Invalid primitive type.");
        }

        final ObjectNode obj = JsonNodeFactory.instance.objectNode();
        obj.set("type", JsonNodeFactory.instance.textNode(type));
        if (type.equals(ElasticsearchSinkConnectorConstants.TEXT_TYPE)) {
            addTextMapping(obj);
        }
        JsonNode defaultValueNode = null;
        if (defaultValue != null) {
            switch (type) {
                case ElasticsearchSinkConnectorConstants.BYTE_TYPE:
                    defaultValueNode = JsonNodeFactory.instance.numberNode((byte) defaultValue);
                    break;
                case ElasticsearchSinkConnectorConstants.SHORT_TYPE:
                    defaultValueNode = JsonNodeFactory.instance.numberNode((short) defaultValue);
                    break;
                case ElasticsearchSinkConnectorConstants.INTEGER_TYPE:
                    defaultValueNode = JsonNodeFactory.instance.numberNode((int) defaultValue);
                    break;
                case ElasticsearchSinkConnectorConstants.LONG_TYPE:
                    defaultValueNode = JsonNodeFactory.instance.numberNode((long) defaultValue);
                    break;
                case ElasticsearchSinkConnectorConstants.FLOAT_TYPE:
                    defaultValueNode = JsonNodeFactory.instance.numberNode((float) defaultValue);
                    break;
                case ElasticsearchSinkConnectorConstants.DOUBLE_TYPE:
                    defaultValueNode = JsonNodeFactory.instance.numberNode((double) defaultValue);
                    break;
                case ElasticsearchSinkConnectorConstants.STRING_TYPE:
                case ElasticsearchSinkConnectorConstants.TEXT_TYPE:
                    defaultValueNode = JsonNodeFactory.instance.textNode((String) defaultValue);
                    break;
                case ElasticsearchSinkConnectorConstants.BINARY_TYPE:
                    defaultValueNode = JsonNodeFactory.instance.binaryNode(bytes(defaultValue));
                    break;
                case ElasticsearchSinkConnectorConstants.BOOLEAN_TYPE:
                    defaultValueNode = JsonNodeFactory.instance.booleanNode((boolean) defaultValue);
                    break;
                case ElasticsearchSinkConnectorConstants.DATE_TYPE:
                    defaultValueNode = JsonNodeFactory.instance.numberNode((long) defaultValue);
                    break;
                default:
                    throw new DataException("Invalid primitive type.");
            }
        }
        if (defaultValueNode != null) {
            obj.set("null_value", defaultValueNode);
        }
        return obj;
    }

    private static void addTextMapping(final ObjectNode obj) {
        // Add additional mapping for indexing, per https://www.elastic.co/blog/strings-are-dead-long-live-strings
        final ObjectNode keyword = JsonNodeFactory.instance.objectNode();
        keyword.set("type", JsonNodeFactory.instance.textNode(ElasticsearchSinkConnectorConstants.KEYWORD_TYPE));
        keyword.set("ignore_above", JsonNodeFactory.instance.numberNode(256));
        final ObjectNode fields = JsonNodeFactory.instance.objectNode();
        fields.set("keyword", keyword);
        obj.set("fields", fields);
    }

    private static byte[] bytes(final Object value) {
        final byte[] bytes;
        if (value instanceof ByteBuffer) {
            final ByteBuffer buffer = ((ByteBuffer) value).slice();
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
        } else if (value instanceof byte[]) {
            bytes = (byte[]) value;
        } else {
            throw new RuntimeException(String.format("Unsupported type: %s", value.getClass()));
        }
        return bytes;
    }

}
