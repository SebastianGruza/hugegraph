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

package org.apache.hugegraph.unit.core;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hugegraph.HugeException;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.testutil.Assert;
import org.apache.hugegraph.unit.BaseUnitTest;
import org.junit.Test;

/**
 * The store wait used on the first boot of a PD cluster (issue #3203):
 * poll the active store count until it reaches pd.initial-store-count,
 * bounded by pd.stores_wait_timeout.
 */
public class GraphManagerStoresWaitTest extends BaseUnitTest {

    @Test
    public void testEnoughStoresReturnsAtOnce() {
        AtomicInteger calls = new AtomicInteger();
        long waited = GraphManager.waitForStores(() -> {
            calls.incrementAndGet();
            return 3;
        }, 3, 60, 1);
        Assert.assertEquals(0L, waited);
        Assert.assertEquals(1, calls.get());
    }

    @Test
    public void testStoresArrivingLaterAreWaitedFor() {
        // 0, 1, 2 stores on the first three polls, then 3: the fourth poll
        // ends the wait; a failed query (-1) counts like a short answer
        int[] answers = {0, -1, 2, 3};
        AtomicInteger calls = new AtomicInteger();
        long waited = GraphManager.waitForStores(() -> answers[Math.min(calls.getAndIncrement(),
                                                         answers.length - 1)],
                                  3, 60, 1);
        Assert.assertEquals(4, calls.get());
        Assert.assertTrue("waited " + waited, waited >= 2 && waited <= 5);
    }

    @Test
    public void testTimeoutNamesTheOption() {
        AtomicInteger calls = new AtomicInteger();
        Assert.assertThrows(HugeException.class, () -> {
            GraphManager.waitForStores(() -> {
                calls.incrementAndGet();
                return 1;
            }, 3, 2, 1);
        }, e -> {
            Assert.assertContains("Timed out after 2s", e.getMessage());
            Assert.assertContains("3 active store(s)", e.getMessage());
            Assert.assertContains("(1 registered)", e.getMessage());
            Assert.assertContains("pd.stores_wait_timeout", e.getMessage());
        });
        Assert.assertTrue("polls " + calls.get(), calls.get() >= 2);
    }
}
