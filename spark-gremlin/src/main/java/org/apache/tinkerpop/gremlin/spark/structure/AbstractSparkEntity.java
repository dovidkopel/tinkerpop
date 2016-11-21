package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.spark.GraphDriver;

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by dkopel on 11/21/16.
 */
public abstract class AbstractSparkEntity<ID> implements Serializable {
    private final ID id;
    private final UUID graphUUID;

    public AbstractSparkEntity(ID id, UUID graphUUID) {
        this.id = id;
        this.graphUUID = graphUUID;
    }

    public ID getId() {
        return id;
    }

    public ID id() {
        return id;
    }

    public UUID getGraphUUID() {
        return graphUUID;
    }

    public AbstractSparkGraph graph() {
        if(getGraphUUID()==null) throw new IllegalArgumentException();
        return (AbstractSparkGraph) GraphDriver.INSTANCE.graph(getGraphUUID());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractSparkEntity)) return false;

        AbstractSparkEntity<?> that = (AbstractSparkEntity<?>) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        return graphUUID != null ? graphUUID.equals(that.graphUUID) : that.graphUUID == null;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (graphUUID != null ? graphUUID.hashCode() : 0);
        return result;
    }
}
