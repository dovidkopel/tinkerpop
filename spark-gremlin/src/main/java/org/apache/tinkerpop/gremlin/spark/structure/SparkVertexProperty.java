package org.apache.tinkerpop.gremlin.spark.structure;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by dkopel on 11/16/16.
 */
public class SparkVertexProperty<ID, V> extends SparkProperty<ID, V> implements VertexProperty<V> {
    protected Map<String, Long> properties = new ConcurrentHashMap<>();
    private final SparkVertex vertex;
    private final V value;

    public SparkVertexProperty(ID id, String label, SparkVertex vertex, V value, Object... propertyKeyValues) {
        super(id, label, vertex, value, vertex.getGraphUUID());
        this.vertex = vertex;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    @Override
    public V value() {
        return value;
    }

    @Override
    public boolean isPresent() {
        return value != null;
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        Long id = graph().nextId();
        SparkProperty p = new SparkProperty(id, key, vertex, value, graphUUID);
        properties.put(key, id);
        graph().addToRDD(Lists.newArrayList(this, p));
        return p;
    }

    @Override
    public void remove() {
        List ids = new ArrayList();
        ids.addAll(properties.values());
        ids.add(id);
        graph().removeFromRDD(ids);
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        return null;
    }
}
