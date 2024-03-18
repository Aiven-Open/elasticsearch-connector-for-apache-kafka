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

import java.util.HashSet;
import java.util.Set;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import co.elastic.clients.elasticsearch._types.mapping.Property;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MappingTest extends ElasticsearchSinkTestBase {

    private static final String INDEX = "kafka-connect";
    private static final String TYPE = "kafka-connect-type";

    @Test
    public void testMapping() throws Exception {
        final Set<String> indices = new HashSet<>();
        indices.add(INDEX);
        client.createIndices(indices);

        final Schema schema = createSchema();
        Mapping.createMapping(client, INDEX, TYPE, schema);

        final Property mapping = Mapping.getMapping(client, INDEX, TYPE);
        assertNotNull(mapping);
        assertTrue(mapping.isObject());
        verifyMapping(schema, mapping);
    }

    protected Schema createSchema() {
        final Schema structSchema = createInnerSchema();
        return SchemaBuilder.struct().name("record")
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("int8", Schema.INT8_SCHEMA)
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
            .field("struct", structSchema)
            .field("decimal", Decimal.schema(2))
            .field("date", Date.SCHEMA)
            .field("time", Time.SCHEMA)
            .field("timestamp", Timestamp.SCHEMA)
            .build();
    }

    private Schema createInnerSchema() {
        return SchemaBuilder.struct().name("inner")
            .field("boolean", Schema.BOOLEAN_SCHEMA)
            .field("bytes", Schema.BYTES_SCHEMA)
            .field("int8", Schema.INT8_SCHEMA)
            .field("int16", Schema.INT16_SCHEMA)
            .field("int32", Schema.INT32_SCHEMA)
            .field("int64", Schema.INT64_SCHEMA)
            .field("float32", Schema.FLOAT32_SCHEMA)
            .field("float64", Schema.FLOAT64_SCHEMA)
            .field("string", Schema.STRING_SCHEMA)
            .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
            .field("decimal", Decimal.schema(2))
            .field("date", Date.SCHEMA)
            .field("time", Time.SCHEMA)
            .field("timestamp", Timestamp.SCHEMA)
            .build();
    }

    private void verifyMapping(final Schema schema, final Property property) {
        final String schemaName = schema.name();
        if (schemaName != null) {
            switch (schemaName) {
                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                case Timestamp.LOGICAL_NAME:
                    assertEquals(ElasticsearchSinkConnectorConstants.DATE_TYPE, property._kind().jsonValue());
                    return;
                case Decimal.LOGICAL_NAME:
                    assertEquals(ElasticsearchSinkConnectorConstants.DOUBLE_TYPE, property._kind().jsonValue());
                    return;
                default:
            }
        }

        final DataConverter converter =
            new DataConverter(true, DataConverter.BehaviorOnNullValues.IGNORE);
        final Schema.Type schemaType = schema.type();
        switch (schemaType) {
            case ARRAY:
                verifyMapping(schema.valueSchema(), property);
                break;
            case MAP:
                final Schema newSchema = converter.preProcessSchema(schema);
                verifyMapping(
                    newSchema.keySchema(),
                    property.object().properties().get(ElasticsearchSinkConnectorConstants.MAP_KEY)
                );
                verifyMapping(
                    newSchema.valueSchema(),
                    property.object().properties().get(ElasticsearchSinkConnectorConstants.MAP_VALUE)
                );
                break;
            case STRUCT:
                for (final Field field : schema.fields()) {
                    verifyMapping(field.schema(), property.object().properties().get(field.name()));
                }
                break;
            default:
                assertEquals(
                    Mapping.getElasticsearchType(client.getVersion(), schemaType),
                    property._kind().jsonValue()
                );
        }
    }
}
