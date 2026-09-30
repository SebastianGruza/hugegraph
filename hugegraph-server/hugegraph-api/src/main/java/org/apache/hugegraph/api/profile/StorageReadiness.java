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

package org.apache.hugegraph.api.profile;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hugegraph.HugeGraph;
import org.apache.hugegraph.auth.HugeGraphAuthProxy;
import org.apache.hugegraph.core.GraphManager;
import org.apache.hugegraph.util.Log;
import org.slf4j.Logger;

/**
 * Whether this server can serve graph traffic, for a readiness probe.
 * Graphs on an embedded backend are ready as soon as the REST layer answers.
 * Graphs on hstore are probed through the backend's "storage_readiness"
 * metadata: at least one known Store answers a cheap direct call; PD is only
 * needed until the first Store list is known. The storage is shared by every
 * hstore graph of the process, so one graph is probed and the result is
 * reused for a short TTL to keep repeated probes cheap. The body never
 * carries raw exception text, since the endpoint is unauthenticated.
 */
public final class StorageReadiness {

    public static final String STORAGE_READINESS_META = "storage_readiness";
    public static final String BACKEND_HSTORE = "hstore";

    private static final Logger LOG = Log.logger(StorageReadiness.class);

    private static volatile Map<String, Object> lastResult;
    private static volatile long lastCheckedAt;
    private static final AtomicReference<CompletableFuture<Map<String, Object>>> IN_FLIGHT =
            new AtomicReference<>();

    private StorageReadiness() {
    }

    /** One storage probe with a time budget in ms. */
    public interface Probe {

        Map<String, Object> probe(long timeoutMs) throws Exception;
    }

    public static Map<String, Object> check(GraphManager manager,
                                            long timeoutMs, long cacheTtlMs) {
        // The graphs are auth proxies and the probe request carries no user,
        // so look the graph up and probe it as the internal admin, the way
        // other internal paths do; the result carries no data or addresses
        List<Map<String, Object>> holder = new ArrayList<>(1);
        HugeGraphAuthProxy.runAsAdmin(() -> {
            HugeGraph graph = firstHstoreGraph(manager);
            if (graph == null) {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("ready", true);
                body.put("storage", "embedded");
                body.put("reason", "no graph on a remote storage");
                holder.add(body);
                return;
            }
            holder.add(check(t -> graph.metadata(null, STORAGE_READINESS_META, t),
                             timeoutMs, cacheTtlMs));
        });
        return holder.get(0);
    }

    /**
     * At most one probe runs at a time and every caller gets its answer:
     * the first caller runs the probe on its own thread, concurrent callers
     * wait for that result, each bounded by its own timeout. No monitor is
     * held during the probe, so a slow store cannot queue REST workers
     * behind it (relevant with readiness.cache_ttl=0, where every request
     * probes).
     */
    public static Map<String, Object> check(Probe probe, long timeoutMs, long cacheTtlMs) {
        long now = System.currentTimeMillis();
        Map<String, Object> cached = lastResult;
        if (cached != null && now - lastCheckedAt < cacheTtlMs) {
            Map<String, Object> body = new LinkedHashMap<>(cached);
            body.put("cached", true);
            return body;
        }
        CompletableFuture<Map<String, Object>> mine = new CompletableFuture<>();
        CompletableFuture<Map<String, Object>> running = IN_FLIGHT.get();
        if (running == null && IN_FLIGHT.compareAndSet(null, mine)) {
            Map<String, Object> body;
            try {
                body = probeOnce(probe, timeoutMs);
                lastResult = body;
                lastCheckedAt = System.currentTimeMillis();
            } finally {
                IN_FLIGHT.set(null);
            }
            mine.complete(body);
            return new LinkedHashMap<>(body);
        }
        if (running == null) {
            running = IN_FLIGHT.get();
        }
        if (running == null) {
            // The owner finished between our two reads: its result is cached
            return check(probe, timeoutMs, cacheTtlMs);
        }
        try {
            Map<String, Object> body = new LinkedHashMap<>(
                    running.get(timeoutMs, TimeUnit.MILLISECONDS));
            body.put("shared", true);
            return body;
        } catch (TimeoutException e) {
            return notReady("a probe is still running after " + timeoutMs + " ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return notReady("interrupted");
        } catch (ExecutionException e) {
            return notReady("probe failed: " + e.getCause().getClass().getSimpleName());
        }
    }

    private static Map<String, Object> probeOnce(Probe probe, long timeoutMs) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ready", false);
        body.put("storage", BACKEND_HSTORE);
        try {
            Map<String, Object> result = probe.probe(timeoutMs);
            body.putAll(result);
        } catch (Throwable e) {
            LOG.warn("Storage readiness probe failed", e);
            body.put("ready", false);
            body.put("reason", "probe failed: " + e.getClass().getSimpleName());
        }
        body.put("cached", false);
        return body;
    }

    private static Map<String, Object> notReady(String reason) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("ready", false);
        body.put("storage", BACKEND_HSTORE);
        body.put("reason", reason);
        body.put("cached", false);
        return body;
    }

    public static boolean isReady(Map<String, Object> body) {
        return Boolean.TRUE.equals(body.get("ready"));
    }

    public static void resetCache() {
        lastResult = null;
        lastCheckedAt = 0L;
        IN_FLIGHT.set(null);
    }

    private static HugeGraph firstHstoreGraph(GraphManager manager) {
        for (String name : manager.graphs()) {
            try {
                HugeGraph graph = manager.graph(name);
                if (graph != null && BACKEND_HSTORE.equals(graph.backend())) {
                    return graph;
                }
            } catch (Throwable e) {
                LOG.debug("Skip graph {} while looking for a remote storage", name, e);
            }
        }
        return null;
    }
}
