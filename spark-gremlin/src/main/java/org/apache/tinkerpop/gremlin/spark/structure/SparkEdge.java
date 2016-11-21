package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkEdge<ID extends Long> extends SparkElement<ID> implements Edge {
    protected Map<String, Property> properties;
    protected final Long _inVertex;
    protected final Long _outVertex;

    protected SparkEdge(final ID id, final SparkVertex outVertex, final String label, final SparkVertex inVertex) {
        super(id, label, inVertex.getGraphUUID());
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
                return graph().getRDD(this._outVertex);
            case IN:
                return graph().getRDD(this._inVertex);
            case BOTH:
            default:
                return graph().getRDD(this._inVertex, this._outVertex);
        }
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SparkEdge)) return false;
        if (!super.equals(o)) return false;

        SparkEdge<?> sparkEdge = (SparkEdge<?>) o;

        if (properties != null ? !properties.equals(sparkEdge.properties) : sparkEdge.properties != null) return false;
        if (_inVertex != null ? !_inVertex.equals(sparkEdge._inVertex) : sparkEdge._inVertex != null) return false;
        return _outVertex != null ? _outVertex.equals(sparkEdge._outVertex) : sparkEdge._outVertex == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        result = 31 * result + (_inVertex != null ? _inVertex.hashCode() : 0);
        result = 31 * result + (_outVertex != null ? _outVertex.hashCode() : 0);
        return result;
    }
}
