package org.apache.tinkerpop.gremlin.spark.structure;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.spark.GraphDriver;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputeComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.SparkInterceptorStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkGraph implements Graph {

    static {
        TraversalStrategies.GlobalCache.registerStrategies(SparkGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(
            SparkInterceptorStrategy.instance())
        );
    }

    private final org.apache.commons.configuration.Configuration hadoopConf = new HadoopConfiguration();
    private final SparkConf sparkConf = new SparkConf();
    private final JavaSparkContext context;
    private final AtomicLong currentId = new AtomicLong(-1L);
    protected final UUID uuid;
    private JavaPairRDD<Long, SparkElement> rdd;
    private Class<? extends GraphComputer> graphComputerClass = SparkGraphComputeComputer.class;

    public SparkGraph() {
        sparkConf.setAppName("test").setMaster("local[4]");
        this.uuid = GraphDriver.INSTANCE.newgraph(this);
        this.context = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
        this.rdd = context.parallelizePairs(Lists.<Tuple2<Long, SparkElement>>newArrayList());
    }

    private <V extends SparkElement> JavaPairRDD<Long, V> parallelize(Long id, V element) {
        return context.parallelizePairs(Lists.newArrayList(new Tuple2<>(id, element)));
    }


    protected Long nextId() {
        return currentId.incrementAndGet();
    }

    protected JavaPairRDD<Long, SparkElement> addToRDD(JavaPairRDD<Long, SparkElement> element) {
        this.rdd = rdd.union(element);
        return this.rdd;
    }

    protected JavaPairRDD<Long, SparkElement> addToRDD(SparkElement element) {
        return addToRDD(parallelize((Long) element.id, element));
    }

    protected JavaPairRDD<Long, SparkElement> addToRDD(SparkElement... elements) {
        Set<Long> ids = new HashSet();
        List<Tuple2<Long, SparkElement>> tuples = Stream.of(elements).map(e -> {
            ids.add((Long) e.id);
            rdd = rdd.subtractByKey(parallelize((Long) e.id, null));
            return new Tuple2<Long, SparkElement>((Long)e.id, e);
        }).collect(Collectors.toList());

        this.rdd = rdd.union(context.parallelizePairs(tuples));
        return rdd;
    }

    protected JavaPairRDD<Long, SparkElement> getRDD() {
        return this.rdd;
    }

    @Override
    public SparkVertex addVertex(Object... keyValues) {
        return SparkUtils.addVertex(this, keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        this.graphComputerClass = graphComputerClass;
        return (C) compute();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        try {
            return (GraphComputer) ConstructorUtils.invokeConstructor(graphComputerClass, this);
        } catch (Exception e) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        Set<Long> ids = new HashSet();
        for(Object v : vertexIds) {
            if(v instanceof Long) {
                ids.add((Long) v);
            } else {
                ids.add(Long.valueOf(v.toString()));
            }
        }
        return this.rdd
            .filter((t) -> SparkUtils.isVertex(t, Sets.newHashSet(ids)))
            .map(t -> (Vertex) t._2())
            .toLocalIterator();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        Set<Long> ids = new HashSet();
        for(Object v : edgeIds) {
            if(v instanceof Long) {
                ids.add((Long) v);
            } else {
                ids.add(Long.valueOf(v.toString()));
            }
        }
        return this.rdd
            .filter((t) -> SparkUtils.isEdge(t, Sets.newHashSet(ids)))
            .map(t -> (Edge) t._2())
            .toLocalIterator();
    }

    @Override
    public Transaction tx() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public Variables variables() {
        return null;
    }

    @Override
    public Configuration configuration() {
        return null;
    }

    public JavaSparkContext getContext() {
        return context;
    }
}
