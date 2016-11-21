package org.apache.tinkerpop.gremlin.spark.structure;

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
}
