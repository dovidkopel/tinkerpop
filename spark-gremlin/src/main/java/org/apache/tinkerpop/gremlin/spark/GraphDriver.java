package org.apache.tinkerpop.gremlin.spark;

import org.apache.tinkerpop.gremlin.spark.structure.SparkGraph;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by dkopel on 11/15/16.
 */
public enum GraphDriver {
    INSTANCE;

    private Map<UUID, SparkGraph> graphs = new ConcurrentHashMap();

    public UUID newgraph(SparkGraph graph) {
        UUID id = UUID.randomUUID();
        graphs.put(id, graph);
        return id;
    }

    public SparkGraph graph(UUID id) {
        return graphs.get(id);
    }
}
