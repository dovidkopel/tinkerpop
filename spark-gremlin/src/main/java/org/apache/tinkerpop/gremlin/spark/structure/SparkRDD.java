package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.commons.lang.ClassUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * Created by dkopel on 11/16/16.
 */
public enum SparkRDD {
    VERTEX,
    EDGE,
    PROPERTY;

    public static SparkRDD findByClass(Class clazz) {
        if(ClassUtils.getAllInterfaces(clazz).contains(Vertex.class)) {
            return SparkRDD.VERTEX;
        } else if(ClassUtils.getAllInterfaces(clazz).contains(Edge.class)) {
            return SparkRDD.EDGE;
        } else if(ClassUtils.getAllInterfaces(clazz).contains(Property.class)) {
            return SparkRDD.PROPERTY;
        }
        throw new IllegalArgumentException();
    }
}
