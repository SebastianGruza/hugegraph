/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hugegraph.api.graph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;

/**
 * Reads the "properties" object of a vertex or edge body so that a JSON
 * fraction keeps every digit: it becomes a BigDecimal instead of a double,
 * which is what a DECIMAL property key needs and what every numeric key
 * narrows through DataType.valueToNumber as before. Only property values
 * are read this way; the rest of the request body (job parameters, schema
 * userdata, query options) keeps Jackson's default number types.
 */
public class PropertiesDeserializer extends JsonDeserializer<Map<String, Object>> {

    @Override
    public Map<String, Object> deserialize(JsonParser parser,
                                           DeserializationContext ctxt)
                                           throws IOException {
        JsonToken token = parser.currentToken();
        if (token == JsonToken.VALUE_NULL) {
            return null;
        }
        if (token != JsonToken.START_OBJECT) {
            throw JsonMappingException.from(parser,
                  "Expected an object for 'properties', but got " + token);
        }
        return readObject(parser);
    }

    private static Map<String, Object> readObject(JsonParser parser)
                                                  throws IOException {
        Map<String, Object> object = new LinkedHashMap<>();
        while (parser.nextToken() != JsonToken.END_OBJECT) {
            String name = parser.currentName();
            parser.nextToken();
            object.put(name, readValue(parser));
        }
        return object;
    }

    private static List<Object> readArray(JsonParser parser)
                                          throws IOException {
        List<Object> array = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
            array.add(readValue(parser));
        }
        return array;
    }

    /** One value at the parser's current token: exact fractions, nested objects and arrays. */
    public static Object readValue(JsonParser parser) throws IOException {
        JsonToken token = parser.currentToken();
        switch (token) {
            case START_OBJECT:
                return readObject(parser);
            case START_ARRAY:
                return readArray(parser);
            case VALUE_STRING:
                return parser.getText();
            case VALUE_NUMBER_INT:
                return parser.getNumberValue();
            case VALUE_NUMBER_FLOAT:
                // Exact: the literal's digits, not the nearest double
                return parser.getDecimalValue();
            case VALUE_TRUE:
                return Boolean.TRUE;
            case VALUE_FALSE:
                return Boolean.FALSE;
            case VALUE_NULL:
                return null;
            default:
                throw JsonMappingException.from(parser,
                      "Unexpected token in 'properties': " + token);
        }
    }
}
