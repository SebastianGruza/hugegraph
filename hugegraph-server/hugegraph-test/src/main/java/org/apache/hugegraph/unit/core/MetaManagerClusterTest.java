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

package org.apache.hugegraph.unit.core;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.hugegraph.config.CoreOptions;
import org.apache.hugegraph.config.HugeConfig;
import org.apache.hugegraph.config.ServerOptions;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.meta.MetaDriver;
import org.apache.hugegraph.meta.MetaManager;
import org.apache.hugegraph.testutil.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * The meta keys in PD are prefixed with the cluster name, and
 * MetaManager.connect() binds that name once per process. A server that
 * connects under the configured 'cluster' must not be preceded by a graph
 * connecting under its own 'pd.cluster', otherwise every later lookup reads
 * an empty tree (1.7.0 wrote the schema under 'hg-test', master looked
 * under 'hg').
 */
public class MetaManagerClusterTest {

    /*
     * connect() also rebuilds every sub-manager of the singleton, so the
     * whole instance state is snapshotted and restored, not just the driver
     * and the cluster; the suite must find the singleton as it left it.
     */
    private final Map<Field, Object> snapshot = new HashMap<>();

    @Before
    public void setup() throws Exception {
        for (Field f : MetaManager.class.getDeclaredFields()) {
            if (Modifier.isStatic(f.getModifiers())) {
                continue;
            }
            f.setAccessible(true);
            this.snapshot.put(f, f.get(MetaManager.instance()));
        }
        swapField("metaDriver", null);
        swapField("cluster", null);
    }

    @After
    public void teardown() throws Exception {
        for (Map.Entry<Field, Object> e : this.snapshot.entrySet()) {
            e.getKey().set(MetaManager.instance(), e.getValue());
        }
    }

    @Test
    public void testFirstConnectWinsAndLaterNamesAreIgnored() throws Exception {
        connectWithMockDriver("hg-test");
        Assert.assertEquals("hg-test", MetaManager.instance().cluster());

        // the graph-level fallback ('pd.cluster', default 'hg') is a no-op
        MetaManager.instance().connect("hg", MetaManager.MetaDriverType.PD,
                                       "ca", "ca", "ca", "127.0.0.1:8686");
        Assert.assertEquals("hg-test", MetaManager.instance().cluster());
        MetaManager.instance().ensureCluster("hg-test");
    }

    @Test
    public void testEnsureClusterFailsOnAnotherName() throws Exception {
        connectWithMockDriver("hg");

        Assert.assertThrows(IllegalStateException.class, () -> {
            MetaManager.instance().ensureCluster("hg-test");
        }, e -> {
            Assert.assertContains("connected to cluster 'hg'", e.getMessage());
            Assert.assertContains("configured cluster is 'hg-test'",
                                  e.getMessage());
            Assert.assertContains("HUGEGRAPH/hg/", e.getMessage());
        });
    }

    @Test
    public void testEnsureClusterRequiresAConnection() {
        Assert.assertThrows(IllegalStateException.class, () -> {
            MetaManager.instance().ensureCluster("hg-test");
        });
    }

    /**
     * The server binds the configured cluster before any graph is opened;
     * a graph that got there first under another name is a startup error,
     * not a silently empty schema.
     */
    @Test
    public void testServerConnectDetectsGraphLevelCluster() throws Exception {
        connectWithMockDriver(CoreOptions.PD_CLUSTER.defaultValue());

        HugeConfig conf = new HugeConfig(new PropertiesConfiguration());
        Assert.assertEquals("hg-test", conf.get(ServerOptions.CLUSTER));
        Assert.assertThrows(IllegalStateException.class, () -> {
            GraphManager.connectMetaManager(conf);
        }, e -> {
            Assert.assertContains("configured cluster is 'hg-test'",
                                  e.getMessage());
        });
    }

    @Test
    public void testServerConnectIsIdempotentUnderTheConfiguredCluster()
                                                       throws Exception {
        connectWithMockDriver("hg-test");
        HugeConfig conf = new HugeConfig(new PropertiesConfiguration());
        GraphManager.connectMetaManager(conf);
        GraphManager.connectMetaManager(conf);
        Assert.assertEquals("hg-test", MetaManager.instance().cluster());
    }

    @Test
    public void testTeardownRestoresTheSubManagers() throws Exception {
        Field f = MetaManager.class.getDeclaredField("authMetaManager");
        f.setAccessible(true);
        Object before = this.snapshot.get(f);
        connectWithMockDriver("hg-test");
        Assert.assertNotSame(before, f.get(MetaManager.instance()));
        teardown();
        Assert.assertSame(before, f.get(MetaManager.instance()));
    }

    @Test
    public void testGraphLevelDefaultStaysHg() {
        Assert.assertEquals("hg", CoreOptions.PD_CLUSTER.defaultValue());
    }

    private static void connectWithMockDriver(String cluster) throws Exception {
        // connect() builds a real driver, so pre-bind a mock one and the
        // cluster the same way the first successful connect() would
        swapField("metaDriver", Mockito.mock(MetaDriver.class));
        swapField("cluster", cluster);
        MetaManager.instance().connect(cluster, MetaManager.MetaDriverType.PD,
                                       null, null, null, "127.0.0.1:8686");
    }

    private static void swapField(String field, Object replacement)
                                  throws Exception {
        Field f = MetaManager.class.getDeclaredField(field);
        f.setAccessible(true);
        f.set(MetaManager.instance(), replacement);
    }
}
