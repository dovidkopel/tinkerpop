package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkVertex extends SparkElement implements Vertex {
    transient private final SparkGraph graph;
    protected Map<String, List<VertexProperty>> properties;
    protected Map<String, Set<SparkEdge>> outEdges = new ConcurrentHashMap<>();
    protected Map<String, Set<SparkEdge>> inEdges = new ConcurrentHashMap<>();

    protected SparkVertex(final Object id, final String label, final SparkGraph graph) {
        super(id, label);
        this.graph = graph;
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        return null;
    }

    @Override
    public SparkEdge addEdge(String label, Vertex inVertex, Object... keyValues) {
        return SparkUtils.addEdge(graph, (SparkVertex) inVertex, this, keyValues);
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return null;
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
