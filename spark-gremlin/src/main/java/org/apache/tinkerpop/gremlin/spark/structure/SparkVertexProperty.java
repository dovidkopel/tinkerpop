package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by dkopel on 11/16/16.
 */
public class SparkVertexProperty<ID, V> extends SparkElement<ID> implements VertexProperty<V> {
    protected Map<String, Property> properties = new ConcurrentHashMap<>();
    private final SparkVertex vertex;
    private final V value;

    public SparkVertexProperty(ID id, String label, SparkVertex vertex, V value, Object... propertyKeyValues) {
        super(id, label, vertex.graphUUID);
        this.vertex = vertex;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    @Override
    public String key() {
        return label;
    }

    @Override
    public V value() {
        return value;
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public Vertex element() {
        return vertex;
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
}
