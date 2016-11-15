package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.spark.GraphDriver;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by dkopel on 11/15/16.
 */
public abstract class SparkElement implements Element, Serializable {
    protected final Object id;
    protected final String label;
    protected final UUID graphUUID;
    protected boolean removed = false;

    protected SparkElement(final Object id, final String label, UUID graphUUID) {
        this.id = id;
        this.label = label;
        this.graphUUID = graphUUID;
    }

    protected SparkElement(final Object id, final String label) {
        this.id = id;
        this.label = label;
        this.graphUUID = null;
    }

    @Override
    public SparkGraph graph() {
        if(graphUUID==null) throw new IllegalArgumentException();
        return GraphDriver.INSTANCE.graph(graphUUID);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public Object id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    protected static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> clazz, final Object id) {
        return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
    }
}
