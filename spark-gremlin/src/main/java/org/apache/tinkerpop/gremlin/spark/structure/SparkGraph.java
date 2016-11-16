package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import scala.Tuple2;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

/**
 * Created by dkopel on 11/16/16.
 */
public interface SparkGraph<ID> extends Graph {
    UUID getUUID();
    ID nextId();
    ID toId(Object id);
    Iterator<Vertex> vertices(Collection<Object> vertexIds);
    Iterator<Edge> edges(Collection<Object> vertexIds);
    JavaSparkContext getContext();
    <T extends SparkElement> void addToRDD(Collection<T> elements);
    JavaPairRDD<ID, SparkVertex> addToRDD(SparkVertex... vertex);
    JavaPairRDD<ID, SparkEdge> addToRDD(SparkEdge... edge);
    Iterator<? extends SparkElement> getRDD(ID... ids);
    JavaPairRDD<ID, SparkVertex> getVertexRDD();
    JavaPairRDD<ID, SparkEdge> getEdgeRDD();

    default Boolean isVertex(Tuple2<ID, ? extends SparkElement> t) {
        return t._2() instanceof SparkVertex;
    }

    default Boolean isVertex(Tuple2<ID, ? extends SparkElement> t, Set<ID> ids) {
        return isVertex(t) && (ids.size() == 0 || ids.contains(t._1()));
    }

    default Boolean isEdge(Tuple2<ID, ? extends SparkElement> t) {
        return t._2() instanceof SparkEdge;
    }

    default Boolean isEdge(Tuple2<ID, ? extends SparkElement> t, Set<ID> ids) {
        return isEdge(t) && (ids.size() == 0 || ids.contains(t._1()));
    }

    default String defaultLabel(Class clazz) {
        switch(clazz.getSimpleName()) {
            case "Vertex":
                return Vertex.DEFAULT_LABEL;
            case "Edge":
                return Edge.DEFAULT_LABEL;

        }
        return null;
    }
}