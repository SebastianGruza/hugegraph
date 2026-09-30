/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hugegraph.api.schema;

import java.io.IOException;
import java.util.Map;

import org.apache.hugegraph.api.graph.PropertiesDeserializer;
import org.apache.hugegraph.schema.Userdata;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

/**
 * The userdata of a property key read like element properties: a JSON
 * fraction becomes a BigDecimal with every digit, so a DECIMAL
 * {@code ~default_value} reaches the property key exactly; everything else
 * keeps Jackson's default types.
 */
public class UserdataDeserializer extends StdDeserializer<Userdata> {

    private static final long serialVersionUID = 1L;

    private static final PropertiesDeserializer PROPERTIES = new PropertiesDeserializer();

    public UserdataDeserializer() {
        super(Userdata.class);
    }

    @Override
    public Userdata deserialize(JsonParser parser, DeserializationContext context)
                                throws IOException {
        Map<String, Object> map = PROPERTIES.deserialize(parser, context);
        return map == null ? null : new Userdata(map);
    }
}
