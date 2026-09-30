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

package org.apache.hugegraph.unit.api.graph;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import org.apache.hugegraph.api.graph.PropertiesDeserializer;
import org.apache.hugegraph.api.schema.UserdataDeserializer;
import org.apache.hugegraph.schema.Userdata;
import org.apache.hugegraph.testutil.Assert;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

public class PropertiesDeserializerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static class Body {

        @JsonProperty("label")
        public String label;
        @JsonProperty("properties")
        @JsonDeserialize(using = PropertiesDeserializer.class)
        public Map<String, Object> properties;
        @JsonProperty("options")
        public Map<String, Object> options;
    }

    @Test
    public void testFractionsAreExactInPropertiesOnly() throws Exception {
        Body body = MAPPER.readValue(
                "{\"label\":\"account\"," +
                "\"properties\":{\"amount\":12345678901234567890.123456789012345678," +
                "\"rate\":1.10,\"count\":7,\"big\":123456789012345678901234567890," +
                "\"name\":\"a\",\"ok\":true,\"none\":null," +
                "\"tags\":[1.5,\"x\",[2.25]],\"nested\":{\"w\":0.1}}," +
                "\"options\":{\"alpha\":0.85}}", Body.class);

        Map<String, Object> props = body.properties;
        Assert.assertEquals(new BigDecimal("12345678901234567890.123456789012345678"),
                            props.get("amount"));
        Assert.assertEquals(new BigDecimal("1.10"), props.get("rate"));
        Assert.assertEquals(7, props.get("count"));
        Assert.assertEquals(new java.math.BigInteger("123456789012345678901234567890"),
                            props.get("big"));
        Assert.assertEquals("a", props.get("name"));
        Assert.assertEquals(Boolean.TRUE, props.get("ok"));
        Assert.assertTrue(props.containsKey("none"));
        Assert.assertNull(props.get("none"));
        List<?> tags = (List<?>) props.get("tags");
        Assert.assertEquals(new BigDecimal("1.5"), tags.get(0));
        Assert.assertEquals("x", tags.get(1));
        Assert.assertEquals(new BigDecimal("2.25"), ((List<?>) tags.get(2)).get(0));
        Assert.assertEquals(new BigDecimal("0.1"),
                            ((Map<?, ?>) props.get("nested")).get("w"));
        // key order is kept
        Assert.assertEquals("amount", props.keySet().iterator().next());

        // everything outside "properties" keeps Jackson's default types
        Assert.assertEquals(0.85d, body.options.get("alpha"));
    }

    public static class KeyBody {

        @JsonProperty("name")
        public String name;
        @JsonProperty("user_data")
        @JsonDeserialize(using = UserdataDeserializer.class)
        public Userdata userdata;
    }

    /** The same reading on a property key's user_data (PropertyKeyAPI). */
    @Test
    public void testPropertyKeyUserdataIsExact() throws Exception {
        KeyBody key = MAPPER.readValue(
                "{\"name\":\"fee\"," +
                "\"user_data\":{\"~default_value\":0.1234567890123456789,\"note\":\"x\"," +
                "\"weight\":2}}", KeyBody.class);
        Assert.assertEquals(new BigDecimal("0.1234567890123456789"),
                            key.userdata.get("~default_value"));
        Assert.assertEquals("x", key.userdata.get("note"));
        Assert.assertEquals(2, key.userdata.get("weight"));
        key = MAPPER.readValue("{\"name\":\"fee\"}", KeyBody.class);
        Assert.assertNull(key.userdata);
    }

    @Test
    public void testNullAndNonObject() throws Exception {
        Body body = MAPPER.readValue("{\"label\":\"x\",\"properties\":null}",
                                     Body.class);
        Assert.assertNull(body.properties);
        Assert.assertThrows(com.fasterxml.jackson.databind.JsonMappingException.class, () -> {
            MAPPER.readValue("{\"label\":\"x\",\"properties\":[1]}", Body.class);
        });
    }
}
