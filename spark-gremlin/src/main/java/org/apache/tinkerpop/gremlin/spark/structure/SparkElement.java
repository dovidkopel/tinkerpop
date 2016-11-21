package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.spark.GraphDriver;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by dkopel on 11/15/16.
 */
public abstract class SparkElement<ID> extends AbstractSparkEntity<ID> implements Element, Serializable {
    protected final String label;
    protected boolean removed = false;

    protected SparkElement(final ID id, final String label, UUID graphUUID) {
        super(id, graphUUID);
        this.label = label;
    }

    protected SparkElement(final ID id, final String label) {
        super(id, null);
        this.label = label;
    }

    @Override
    public AbstractSparkGraph graph() {
        if(getGraphUUID()==null) throw new IllegalArgumentException();
        return (AbstractSparkGraph) GraphDriver.INSTANCE.graph(getGraphUUID());
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
            "id=" + getId() +
            ", label='" + label + '\'' +
            ", graphUUID=" + getGraphUUID() +
            ", removed=" + removed +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SparkElement)) return false;

        SparkElement<?> that = (SparkElement<?>) o;

        if (removed != that.removed) return false;
        if (!getId().equals(that.getId())) return false;
        if (!label.equals(that.label)) return false;
        return getGraphUUID().equals(that.getGraphUUID());

    }

    @Override
    public int hashCode() {
        int result = getId().hashCode();
        result = 31 * result + label.hashCode();
        result = 31 * result + getGraphUUID().hashCode();
        result = 31 * result + (removed ? 1 : 0);
        return result;
    }
}
