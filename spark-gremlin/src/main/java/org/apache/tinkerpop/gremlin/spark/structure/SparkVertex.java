package org.apache.tinkerpop.gremlin.spark.structure;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkVertex extends SparkElement implements Vertex {
    protected Map<String, List<VertexProperty>> properties;
    protected Map<String, Set<Long>> outEdges = new ConcurrentHashMap<>();
    protected Map<String, Set<Long>> inEdges = new ConcurrentHashMap<>();

    public SparkVertex(final Object id, final String label, final SparkGraph graph) {
        super(id, label, graph.uuid);
    }

    public SparkVertex(Object id, String label, UUID graphUUID) {
        super(id, label, graphUUID);
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
        List args = Lists.newArrayList();
        args.addAll(Arrays.asList(keyValues));
        if(!args.contains(T.label)) {
            args.add(T.label);
            args.add(label);
        }
        return SparkUtils.addEdge(graph(), (SparkVertex) inVertex, this, args.toArray());
    }

    protected void _addEdge(Direction direction, String label, Long id) {
        if (_edges(direction).containsKey(label)) {
            _edges(direction).get(label).add(id);
        } else {
            _edges(direction).put(label, Sets.newHashSet(id));
        }
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        return null;
    }

    private Map<String, Set<Long>> _edges(Direction direction) {
        switch(direction) {
            case OUT:
                return outEdges;
            case IN:
                return inEdges;
            case BOTH:
            default:
                return ImmutableMap.<String, Set<Long>>builder().putAll(inEdges).putAll(outEdges).build();
        }
    }

    private Set<Vertex> _vertexes(Direction direction, Edge e) {
        switch(direction) {
            case OUT:
                return Sets.newHashSet(e.inVertex());
            case IN:
                return Sets.newHashSet(e.outVertex());
            case BOTH:
            default:
                return Sets.newHashSet(e.bothVertices());
        }
    }

    private Set<Long> _edgesIds(Direction direction, String... edgeLabels) {
        return _edges(direction).entrySet()
            .stream()
            .filter(e -> edgeLabels.length == 0 || Arrays.asList(edgeLabels).contains(e.getKey()))
            .map(e -> e.getValue().iterator().next())
            .collect(Collectors.toSet());
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        Set<Long> ids = _edgesIds(direction, edgeLabels);
        return graph().getRDD()
            .filter(t -> ids.contains(t._1()))
            .map(t -> (Edge) t._2())
            .toLocalIterator();
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        Set<Long> ids = _edgesIds(direction, edgeLabels);
        return graph().getRDD()
            .filter(t -> ids.contains(t._1()))
            .flatMap(t -> _vertexes(direction, (Edge) t._2()))
            .toLocalIterator();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
