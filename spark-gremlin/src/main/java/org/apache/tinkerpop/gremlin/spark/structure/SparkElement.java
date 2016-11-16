package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.spark.GraphDriver;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by dkopel on 11/15/16.
 */
public abstract class SparkElement<ID> implements Element, Serializable {
    protected final ID id;
    protected final String label;
    protected final UUID graphUUID;
    protected boolean removed = false;

    protected SparkElement(final ID id, final String label, UUID graphUUID) {
        this.id = id;
        this.label = label;
        this.graphUUID = graphUUID;
    }

    protected SparkElement(final ID id, final String label) {
        this.id = id;
        this.label = label;
        this.graphUUID = null;
    }

    @Override
    public AbstractSparkGraph graph() {
        if(graphUUID==null) throw new IllegalArgumentException();
        return (AbstractSparkGraph) GraphDriver.INSTANCE.graph(graphUUID);
    }

    @Override
    public ID id() {
        return this.id;
    }

    @Override
    public String label() {
        return this.label;
    }

    protected static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> clazz, final Object id) {
        return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
    }

    @Override
    public String toString() {
        return "SparkElement{" +
            "id=" + id +
            ", label='" + label + '\'' +
            ", graphUUID=" + graphUUID +
            ", removed=" + removed +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SparkElement)) return false;

        SparkElement<?> that = (SparkElement<?>) o;

        if (removed != that.removed) return false;
        if (!id.equals(that.id)) return false;
        if (!label.equals(that.label)) return false;
        return graphUUID.equals(that.graphUUID);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + label.hashCode();
        result = 31 * result + graphUUID.hashCode();
        result = 31 * result + (removed ? 1 : 0);
        return result;
    }
}
