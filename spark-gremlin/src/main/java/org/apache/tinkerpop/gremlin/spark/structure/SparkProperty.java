package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Created by dkopel on 11/16/16.
 */
public class SparkProperty<ID, V> implements Property<V> {
    protected final ID id;
    protected final Element element;
    protected final String key;
    protected V value;
    protected final UUID graphUUID;

    protected SparkProperty(ID id, String key, Element element, V value, UUID graphUUID) {
        this.id = id;
        this.element = element;
        this.key = key;
        this.value = value;
        this.graphUUID = graphUUID;
    }

    protected SparkProperty(ID id, String key, V value) {
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
}
