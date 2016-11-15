package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkEdge extends SparkElement implements Edge {
    protected Map<String, Property> properties;
    protected final Long _inVertex;
    protected final Long _outVertex;

    protected SparkEdge(final Object id, final SparkVertex outVertex, final String label, final SparkVertex inVertex) {
        super(id, label, inVertex.graphUUID);
        this._outVertex = (Long) outVertex.id();
        this._inVertex = (Long) inVertex.id();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        switch (direction) {
            case OUT:
                return IteratorUtils.map(graph().getRDD().lookup(this._outVertex).iterator(), e -> (Vertex) e);
            case IN:
                return IteratorUtils.map(graph().getRDD().lookup(this._inVertex).iterator(), e -> (Vertex) e);
            case BOTH:
            default:
                return IteratorUtils.concat(
                    graph().getRDD().lookup(this._outVertex).stream().map(e -> (Vertex) e).iterator(),
                    graph().getRDD().lookup(this._inVertex).stream().map(e -> (Vertex) e).iterator()
                );
        }
    }
//
//    @Override
//    public Vertex outVertex() {
//        List<SparkElement> vs = graph().getRDD().lookup(_outVertex);
//        if(vs.size() == 1) {
//            return (Vertex) vs.get(0);
//        }
//        return null;
//    }
//
//    @Override
//    public Vertex inVertex() {
//        List<SparkElement> vs = graph().getRDD().lookup(_inVertex);
//        if(vs.size() == 1) {
//            return (Vertex) vs.get(0);
//        }
//        return null;
//    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
