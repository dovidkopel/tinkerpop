package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkUtils implements Serializable {

    public static SparkVertex addVertex(SparkGraph graph, Object... keyValues) {
        Tuple2<Long, String> t = prepareElement(graph, Vertex.class, keyValues);
        SparkVertex vertex = new SparkVertex(t._1(), t._2(), graph);
        graph.addToRDD(vertex);
        return vertex;
    }

    public static SparkEdge addEdge(SparkGraph graph, SparkVertex in, SparkVertex out, Object... keyValues) {
        Tuple2<Long, String> t = prepareElement(graph, Edge.class, keyValues);
        SparkEdge edge = new SparkEdge(t._1(), out, t._2(), in);
        in._addEdge(Direction.IN, t._2(), t._1());
        out._addEdge(Direction.OUT, t._2(), t._1());
        graph.addToRDD(edge, in, out);
        return edge;
    }

    private static String defaultLabel(Class clazz) {
        switch(clazz.getSimpleName()) {
            case "Vertex":
                return Vertex.DEFAULT_LABEL;
            case "Edge":
                return Edge.DEFAULT_LABEL;

        }
        return null;
    }

    public static Tuple2<Long, String> prepareElement(SparkGraph graph, Class<? extends Element> clazz, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        Long id;
        if(null == idValue || idValue == Optional.empty()) {
            id = graph.nextId();
        } else {
            id = Long.valueOf(idValue.toString());
        }

        final String label = ElementHelper.getLabelValue(keyValues).orElse(defaultLabel(clazz));
        return new Tuple2(id, label);
    }

    public static Boolean isVertex(Tuple2<Long, SparkElement> t) {
        return t._2() instanceof SparkVertex;
    }

    public static Boolean isVertex(Tuple2<Long, SparkElement> t, Set<Object> ids) {
        return isVertex(t) && (ids.size() == 0 || ids.contains(t._1()));
    }

    public static Boolean isEdge(Tuple2<Long, SparkElement> t) {
        return t._2() instanceof SparkEdge;
    }

    public static Boolean isEdge(Tuple2<Long, SparkElement> t, Set<Object> ids) {
        return isEdge(t) && (ids.size() == 0 || ids.contains(t._1()));
    }
}
