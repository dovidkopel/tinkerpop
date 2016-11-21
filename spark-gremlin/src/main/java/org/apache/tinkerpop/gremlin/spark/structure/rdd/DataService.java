package org.apache.tinkerpop.gremlin.spark.structure.rdd;

import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tinkerpop.gremlin.spark.structure.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Created by dkopel on 11/17/16.
 */
public class DataService<ID extends Long> {
    transient final Logger logger = LoggerFactory.getLogger(getClass());
    private final GraphRDDManager manager;
    final private Map<ID, SparkRDD> rddMap = new ConcurrentHashMap<ID, SparkRDD>();
    final private Map<SparkRDD, UUID> rddTypes = new ConcurrentHashMap<>();

    public DataService(GraphRDDManager manager, Class clazz) {
        this.manager = manager;
        rddTypes.put(SparkRDD.VERTEX, manager.newPairRDD(clazz, SparkVertex.class, "vertex")._1());
        rddTypes.put(SparkRDD.EDGE, manager.newPairRDD(clazz, SparkEdge.class, "edge")._1());
        rddTypes.put(SparkRDD.PROPERTY, manager.newPairRDD(clazz, SparkProperty.class, "property")._1());
    }

    public JavaPairRDD<ID, SparkVertex> getVertexRDD() {
        return manager.getPairRDD(rddTypes.get(SparkRDD.VERTEX));
    }

    public JavaPairRDD<ID, SparkEdge> getEdgeRDD() {
        return manager.getPairRDD(rddTypes.get(SparkRDD.EDGE));
    }

    public JavaPairRDD<ID, SparkProperty> getPropertyRDD() {
        return manager.getPairRDD(rddTypes.get(SparkRDD.PROPERTY));
    }

    private <K extends ID, T extends AbstractSparkEntity> JavaPairRDD<K, T> getRDD(K id) {
        switch(rddMap.get(id)) {
            case VERTEX:
                return manager.getPairRDD(rddTypes.get(SparkRDD.VERTEX));
            case EDGE:
                return manager.getPairRDD(rddTypes.get(SparkRDD.EDGE));
            case PROPERTY:
                return manager.getPairRDD(rddTypes.get(SparkRDD.PROPERTY));
        }
        return null;
    }

    public Iterator<? extends AbstractSparkEntity> getRDD(Collection<ID> ids) {
        return ids.stream()
            .filter(id -> rddMap.containsKey(id))
            .map(id -> getRDD(id).lookup(id).get(0))
            .iterator();
    }

    private <K, V> void invoke(UUID id, Function<JavaPairRDD<K, V>, JavaPairRDD<K, V>> function) throws Exception {
        manager.setPairRDD(id, function.apply(manager.getPairRDD(id)));
    }

    public <K extends UUID, V extends SparkVertex> void invokeVertex(Function<JavaPairRDD<K, V>, JavaPairRDD<K, V>> function) throws Exception {
        invoke(rddTypes.get(SparkRDD.VERTEX), function);
    }

    public <K extends UUID, V extends SparkEdge> void invokeEdge(Function<JavaPairRDD<K, V>, JavaPairRDD<K, V>> function) throws Exception {
        invoke(rddTypes.get(SparkRDD.EDGE), function);
    }

    public JavaPairRDD getRDD(SparkRDD rddType) {
        return manager.getPairRDD(rddTypes.get(rddType));
    }

    public <T extends AbstractSparkEntity<ID>> JavaPairRDD<ID, T> addElement(Class<?> ec, AbstractSparkEntity<ID>... elements) {
        return addElement(ec, Lists.newArrayList(elements));
    }

    public <T extends AbstractSparkEntity<ID>> JavaPairRDD<ID, T> addElement(Class<?> ec, Collection<? extends AbstractSparkEntity<ID>> elements) {
        SparkRDD rddType = SparkRDD.findByClass(ec);
        JavaPairRDD<ID, T>[] vr = new JavaPairRDD[] { getRDD(rddType) };
        elements
            .stream()
            .forEach(v -> {
                rddMap.put(v.id(), rddType);
                vr[0] = vr[0].filter((Tuple2<ID, T> ov) -> !ov._1().equals(v.id())).union(manager.parallelizePairs(Lists.newArrayList(new Tuple2(v.id(), v))));
            });
        manager.setPairRDD(rddTypes.get(rddType), vr[0]);
        return vr[0];
    }

    public JavaPairRDD<ID, SparkVertex<ID>> addVertexes(Collection<SparkVertex<ID>> vertexes) {
        return addElement(SparkVertex.class, vertexes);
    }

    public JavaPairRDD<ID, SparkVertex<ID>> addVertexes(SparkVertex<ID>... vertexes) {
        return addVertexes(Lists.newArrayList(vertexes));
    }

    public JavaPairRDD<ID, SparkEdge<ID>> addEdges(Collection<SparkEdge<ID>> edges) {
        return addElement(SparkEdge.class, edges);
    }

    public JavaPairRDD<ID, SparkEdge<ID>> addEdges(SparkEdge<ID>... edges) {
        return addEdges(Lists.newArrayList(edges));
    }

    public JavaPairRDD<ID, SparkProperty<ID, ?>> addProperties(Collection<SparkProperty<ID, ?>> properties) {
        return addElement(SparkProperty.class, properties);
    }

    public JavaPairRDD<ID, SparkProperty<ID, ?>> addProperties(SparkProperty<ID, ?>... properties) {
        return addProperties(Lists.newArrayList(properties));
    }
}
