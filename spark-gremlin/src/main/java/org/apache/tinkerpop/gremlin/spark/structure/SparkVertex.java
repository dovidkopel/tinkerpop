package org.apache.tinkerpop.gremlin.spark.structure;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkVertex<ID extends Long> extends SparkElement<ID> implements Vertex {
    protected Map<String, List<Long>> properties = new ConcurrentHashMap<>();
    protected Map<String, Set<Long>> outEdges = new ConcurrentHashMap<>();
    protected Map<String, Set<Long>> inEdges = new ConcurrentHashMap<>();

    public SparkVertex(final ID id, final String label, final SparkGraph graph) {
        super(id, label, graph.getUUID());
    }

    public SparkVertex(ID id, String label, UUID graphUUID) {
        super(id, label, graphUUID);
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        Long[] ids;
        if(propertyKeys != null && propertyKeys.length > 0) {
             ids = Stream.of(propertyKeys)
                .filter(p -> properties.containsKey(p))
                .flatMapToLong(p -> properties.get(p).stream().mapToLong(Long::longValue))
                .boxed()
                .toArray(Long[]::new);
        } else {
            ids = properties.values().stream()
                .flatMapToLong(v -> v.stream().mapToLong(Long::longValue))
                .boxed()
                .toArray(Long[]::new);
        }

        if(ids != null && ids.length > 0) {
            LoggerFactory.getLogger(getClass()).debug("Found {} ids", ids.length);
            return graph().getRDD(ids);
        }
        return Iterators.emptyIterator();
    }

    @Override
    public SparkEdge addEdge(String label, Vertex inVertex, Object... keyValues) {
        List args = Lists.newArrayList();
        args.addAll(Arrays.asList(keyValues));
        if(!args.contains(T.label)) {
            args.add(T.label);
            args.add(label);
        }
        return graph().addEdge((SparkVertex) inVertex, this, args.toArray());
    }

    protected void _addEdge(Direction direction, String label, ID id) {
        if (_edges(direction).containsKey(label)) {
            _edges(direction).get(label).add(id);
        } else {
            _edges(direction).put(label, Sets.newHashSet(id));
        }
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id());
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        ElementHelper.validateProperty(key, value);
        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);
        final Optional<VertexProperty<V>> optionalVertexProperty = ElementHelper.stageVertexProperty(this, cardinality, key, value, keyValues);
        if (optionalVertexProperty.isPresent()) return optionalVertexProperty.get();

        final Long idValue = optionalId.isPresent() ? graph().toId(optionalId.get()) : graph().nextId();
        final SparkVertexProperty<Long, V> vertexProperty = new SparkVertexProperty(idValue, key, this, value, keyValues);

        final List<Long> list = this.properties.getOrDefault(key, new ArrayList<>());
        list.add(idValue);
        this.properties.put(key, list);
        graph().addToRDD(Arrays.asList(vertexProperty, this));
        return vertexProperty;
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        if (this.removed) throw elementAlreadyRemoved(Vertex.class, id());

        final Long idValue = graph().nextId();
        final SparkVertexProperty<Long, V> vertexProperty = new SparkVertexProperty(idValue, key, this, value);

        final List<Long> list = this.properties.getOrDefault(key, new ArrayList<>());
        list.add(idValue);
        this.properties.put(key, list);
        graph().addToRDD(Arrays.asList(vertexProperty, this));
        return vertexProperty;
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

    private Set<ID> _edgesIds(Direction direction, String... edgeLabels) {
        return _edges(direction).entrySet()
            .stream()
            .filter(e -> edgeLabels.length == 0 || Arrays.asList(edgeLabels).contains(e.getKey()))
            .map(e -> (ID) e.getValue().iterator().next())
            .collect(Collectors.toSet());
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... edgeLabels) {
        Set<ID> ids = _edgesIds(direction, edgeLabels);
        return graph().edges(ids);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        return IteratorUtils.list(edges(direction, edgeLabels))
            .stream()
            .flatMap(e -> _vertexes(direction, e).stream())
            .iterator();
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SparkVertex)) return false;
        if (!super.equals(o)) return false;

        SparkVertex<?> that = (SparkVertex<?>) o;

        if (properties != null ? !properties.equals(that.properties) : that.properties != null) return false;
        if (outEdges != null ? !outEdges.equals(that.outEdges) : that.outEdges != null) return false;
        return inEdges != null ? inEdges.equals(that.inEdges) : that.inEdges == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        result = 31 * result + (outEdges != null ? outEdges.hashCode() : 0);
        result = 31 * result + (inEdges != null ? inEdges.hashCode() : 0);
        return result;
    }

    @Override
    public Set<String> keys() {
        return properties.keySet();
    }
}
