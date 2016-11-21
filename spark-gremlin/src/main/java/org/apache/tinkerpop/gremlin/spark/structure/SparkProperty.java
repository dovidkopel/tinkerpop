package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Created by dkopel on 11/16/16.
 */
public class SparkProperty<ID, V> extends AbstractSparkEntity<ID> implements Property<V> {
    protected final ID id;
    protected final SparkElement element;
    protected final String key;
    protected V value;
    protected final UUID graphUUID;

    protected SparkProperty(ID id, String key, SparkElement element, V value, UUID graphUUID) {
        super(id, graphUUID);
        this.id = id;
        this.element = element;
        this.key = key;
        this.value = value;
        this.graphUUID = graphUUID;
    }

    protected SparkProperty(ID id, String key, V value) {
        super(id, null);
        this.id = id;
        this.key = key;
        this.value = value;
        this.element = null;
        this.graphUUID = null;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return value != null;
    }

    @Override
    public Element element() {
        return element;
    }

    @Override
    public void remove() {

    }

    public String toString() {
        return StringFactory.propertyString(this);
    }
}
