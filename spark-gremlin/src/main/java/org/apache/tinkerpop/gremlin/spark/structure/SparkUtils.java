package org.apache.tinkerpop.gremlin.spark.structure;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.spark.structure.SparkElement;
import org.apache.tinkerpop.gremlin.spark.structure.SparkGraph;
import org.apache.tinkerpop.gremlin.spark.structure.SparkVertex;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkUtils implements Serializable {

    public static SparkVertex addVertex(SparkGraph graph, Object... keyValues) {
        Tuple2<Long, String> t = prepareElement(graph, keyValues);
        SparkVertex vertex = new SparkVertex(t._1(), t._2(), graph);
        graph.addToRDD(vertex);
        return vertex;
    }

    public static SparkEdge addEdge(SparkGraph graph, SparkVertex in, SparkVertex out, Object... keyValues) {
        Tuple2<Long, String> t = prepareElement(graph, keyValues);
        SparkEdge edge = new SparkEdge(t._1(), out, t._2(), in);
        in.inEdges.put(t._2(), Sets.newHashSet(edge));
        out.outEdges.put(t._2(), Sets.newHashSet(edge));
        graph.addToRDD(edge);
        return edge;
    }

    public static Tuple2<Long, String> prepareElement(SparkGraph graph, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        Long id;
        if(null == idValue || idValue == Optional.empty()) {
            id = graph.nextId();
        } else {
            id = Long.valueOf(idValue.toString());
        }

        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);
        return new Tuple2(id, label);
    }

    public static Boolean isVertex(Tuple2<Long, SparkElement> t) {
        return t._2() instanceof SparkVertex;
    }

    public static Boolean isVertex(Tuple2<Long, SparkElement> t, Set<Object> ids) {
        return isVertex(t) && (ids.size() == 0 || ids.contains(t._1()));
    }
}
