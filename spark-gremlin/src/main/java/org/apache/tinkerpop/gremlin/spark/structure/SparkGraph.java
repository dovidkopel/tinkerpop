package org.apache.tinkerpop.gremlin.spark.structure;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import scala.Tuple2;
import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkGraph implements Graph {
    private final org.apache.commons.configuration.Configuration hadoopConf = new HadoopConfiguration();
    private final SparkConf sparkConf = new SparkConf();
    private final JavaSparkContext context;
    private final AtomicLong currentId = new AtomicLong(-1L);
    private JavaPairRDD<Long, SparkElement> rdd;

    public SparkGraph() {
        sparkConf.setAppName("test").setMaster("local[4]");
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

    @Override
    public SparkVertex addVertex(Object... keyValues) {
        return SparkUtils.addVertex(this, keyValues);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return null;
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return null;
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
        return null;
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
}
