package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkEdge extends SparkElement implements Edge {
    protected Map<String, Property> properties;
    protected final Vertex inVertex;
    protected final Vertex outVertex;

    protected SparkEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex) {
        super(id, label);
        this.outVertex = outVertex;
        this.inVertex = inVertex;
    }

    @Override
    public Graph graph() {
        return this.inVertex.graph();
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
                return IteratorUtils.of(this.outVertex);
            case IN:
                return IteratorUtils.of(this.inVertex);
            default:
                return IteratorUtils.of(this.outVertex, this.inVertex);
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
