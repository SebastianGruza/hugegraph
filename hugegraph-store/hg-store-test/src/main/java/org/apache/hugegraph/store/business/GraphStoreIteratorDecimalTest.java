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

package org.apache.hugegraph.store.business;

import java.math.BigDecimal;
import java.util.LinkedHashSet;

import org.apache.hugegraph.store.grpc.Graphpb;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * The Store-side wire contract of a DECIMAL property: a VT_STRING variant
 * with the plain form of a scalar, or a JSON array of plain strings for a
 * LIST/SET value, exactly as the Server's GraphSON writes them.
 */
public class GraphStoreIteratorDecimalTest {

    @Test
    public void testScalarPayload() {
        Graphpb.Variant v = GraphStoreIterator.decimalVariant(new BigDecimal("1.50"));
        Assert.assertEquals(Graphpb.VariantType.VT_STRING, v.getType());
        Assert.assertEquals("1.50", v.getValueString());
        Assert.assertEquals("12345678901234567890.123456789012345678",
                            GraphStoreIterator.decimalString(
                                    new BigDecimal("12345678901234567890.123456789012345678")));
        // no exponent, whatever the scale
        Assert.assertEquals("0.000000000000000001",
                            GraphStoreIterator.decimalString(new BigDecimal("1E-18")));
        Assert.assertEquals("1000", GraphStoreIterator.decimalString(new BigDecimal("1E+3")));
    }

    @Test
    public void testCollectionPayload() {
        Graphpb.Variant list = GraphStoreIterator.decimalVariant(
                ImmutableList.of(new BigDecimal("1.5"), new BigDecimal("2"), new BigDecimal("1E+3")));
        Assert.assertEquals(Graphpb.VariantType.VT_STRING, list.getType());
        Assert.assertEquals("[\"1.5\",\"2\",\"1000\"]", list.getValueString());
        Graphpb.Variant set = GraphStoreIterator.decimalVariant(
                new LinkedHashSet<>(ImmutableSet.of(new BigDecimal("0.1"), new BigDecimal("0.25"))));
        Assert.assertEquals("[\"0.1\",\"0.25\"]", set.getValueString());
        Assert.assertEquals("[]", GraphStoreIterator.decimalString(ImmutableList.of()));
    }
}
