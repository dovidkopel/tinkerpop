package org.apache.tinkerpop.gremlin.spark.structure;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.ClassUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.spark.GraphDriver;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.TypeVariable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Created by dkopel on 11/16/16.
 */
public abstract class AbstractSparkGraph<ID extends Long> implements SparkGraph<ID> {
    transient final Logger logger = LoggerFactory.getLogger(getClass());
    final Class<ID> clazz;
    private final SparkConf sparkConf;
    private final JavaSparkContext context;
    private final AtomicLong currentId = new AtomicLong(-1L);
    private final UUID uuid;
    private Map<ID, SparkRDD> rddMap = new ConcurrentHashMap<ID, SparkRDD>();
    private JavaPairRDD<ID, SparkVertex> vertexRDD;
    private JavaPairRDD<ID, SparkEdge> edgeRDD;

    public AbstractSparkGraph(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
        this.uuid = GraphDriver.INSTANCE.newgraph(this);
        this.context = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));

        Class c = Object.class;
        for(TypeVariable t : this.getClass().getTypeParameters()) {
            for(AnnotatedType b : t.getAnnotatedBounds()) {
                 c = (Class) b.getType();
            }
        }
        this.clazz = c;
        newGraph();
    }

    private void newGraph() {
        vertexRDD = JavaPairRDD.fromJavaRDD(context.emptyRDD());
        edgeRDD = JavaPairRDD.fromJavaRDD(context.emptyRDD());
    }

    @Override
    public UUID getUUID() {
        return uuid;
    }

    @Override
    public Iterator<? extends SparkElement> getRDD(ID... ids) {
        return Arrays.asList(ids)
            .stream()
            .filter(id -> rddMap.containsKey(id))
            .map(id -> {
            SparkRDD rddType = rddMap.get(id);
            if(rddType != null) {
                switch(rddType) {
                    case VERTEX:
                        List<SparkVertex> v = vertexRDD.lookup(id);
                        if(v != null && v.size() == 1) {
                            return v.get(0);
                        } else {
                            logger.error("The vertex {} returned {} elements", id, v.size());
                            return null;
                        }
                    case EDGE:
                        List<SparkEdge> e = edgeRDD.lookup(id);
                        if(e != null && e.size() == 1) {
                            return e.get(0);
                        } else {
                            logger.error("The edge {} returned {} elements", id, e.size());
                            return null;
                        }
                    default:
                        logger.error("The RDD type for the id {} is {}", id, rddType);
                        throw new IllegalArgumentException();
                }
            }
            logger.error("No RDD type was found for the id {}", id);
            throw new IllegalArgumentException();
        }).iterator();
    }

    protected <T extends SparkElement> Iterator<T> filterElements(Collection<Object> elementIds, JavaPairRDD<ID, T> rdd) {
        Set<ID> ids = elementIds.stream().map(id -> toId(id)).collect(Collectors.toSet());
        logger.debug("Filtering elements ids: {}", ids);
        return rdd
            .filter((t) -> Sets.newHashSet(ids).contains(t._1()))
            .map(t -> t._2())
            .toLocalIterator();
    }

    @Override
    public JavaPairRDD<ID, SparkVertex> getVertexRDD() {
        return vertexRDD;
    }

    @Override
    public JavaPairRDD<ID, SparkEdge> getEdgeRDD() {
        return edgeRDD;
    }

    @Override
    public ID nextId() {
        return (ID) ((Long) currentId.incrementAndGet());
    }

    protected <V extends SparkElement> JavaPairRDD<ID, V> parallelize(ID id, V element) {
        return context.parallelizePairs(Lists.newArrayList(new Tuple2<>(id, element)));
    }

    private <T extends SparkElement> List<Tuple2<ID, T>> tuplize(Class<T> clazz, T... elements) {
        List<T> es = Arrays.asList(elements);

        return es.stream().map(e -> {
            ID id = toId(e.id);
            if(ClassUtils.getAllInterfaces(clazz).contains(Vertex.class)) {
                this.vertexRDD = vertexRDD.filter(t -> !t._1().equals(id)).cache();
                rddMap.put(id, SparkRDD.VERTEX);
            } else if(ClassUtils.getAllInterfaces(clazz).contains(Edge.class)) {
                this.edgeRDD = edgeRDD.filter(t -> !t._1().equals(id)).cache();
                rddMap.put(id, SparkRDD.EDGE);
            } else {
                logger.error("The class for the element {} is {}", id, clazz);
                throw new IllegalArgumentException();
            }

            return new Tuple2<>(id, e);
        }).collect(Collectors.toList());
    }

    protected Tuple2<ID, String> prepareElement(Class<? extends Element> clazz, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        ID id;
        if(null == idValue || idValue == Optional.empty()) {
            id = nextId();
        } else {
            id = toId(idValue);
        }
        logger.debug("prepare element id: {}", id);

        final String label = ElementHelper.getLabelValue(keyValues).orElse(defaultLabel(clazz));
        return new Tuple2(id, label);
    }

    @Override
    public SparkVertex addVertex(Object... keyValues) {
        Tuple2<ID, String> t = prepareElement(Vertex.class, keyValues);
        SparkVertex vertex = new SparkVertex(t._1(), t._2(), this);
        addToRDD(vertex);
        return vertex;
    }

    protected SparkEdge addEdge(SparkVertex in, SparkVertex out, Object... keyValues) {
        Tuple2<ID, String> t = prepareElement(Edge.class, keyValues);
        SparkEdge edge = new SparkEdge(t._1(), out, t._2(), in);
        in._addEdge(Direction.IN, t._2(), t._1());
        out._addEdge(Direction.OUT, t._2(), t._1());
        addToRDD(Lists.newArrayList(edge, in, out));
        return edge;
    }

    @Override
    public <T extends SparkElement> void addToRDD(Collection<T> elements) {
        elements.stream().filter(e -> e instanceof SparkVertex)
            .forEach(e -> addToRDD((SparkVertex)e) );

        elements.stream().filter(e -> e instanceof SparkEdge)
            .forEach(e -> addToRDD((SparkEdge)e) );
    }

    @Override
    public JavaPairRDD<ID, SparkVertex> addToRDD(SparkVertex... vertexes) {
        JavaPairRDD<ID, SparkVertex> es = context.parallelizePairs(tuplize(SparkVertex.class, vertexes));
        this.vertexRDD = vertexRDD.union(es).cache();
        return vertexRDD;
    }

    @Override
    public JavaPairRDD<ID, SparkEdge> addToRDD(SparkEdge... edges) {
        JavaPairRDD<ID, SparkEdge> es = context.parallelizePairs(tuplize(SparkEdge.class, edges));
        this.edgeRDD = edgeRDD.union(es);
        this.edgeRDD.cache();
        return edgeRDD;
    }

    @Override
    public JavaSparkContext getContext() {
        return context;
    }
}
