package org.apache.tinkerpop.gremlin.spark.structure.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by dkopel on 11/17/16.
 */
public class GraphRDDManager implements RDDManager {
    private final SparkConf sparkConf;
    private final JavaSparkContext context;
    private final Map<UUID, RDDContainer> rddMap = new ConcurrentHashMap<>();

    private class RDDContainer {
        final UUID id;
        final String name;
        JavaRDD<?> rdd;
        String path;

        public RDDContainer(UUID id, String name, JavaRDD rdd) {
            this.id = id;
            this.name = name;
            this.rdd = rdd;
        }
    }

    public GraphRDDManager(SparkConf sparkConf) {
        this.sparkConf = sparkConf;
        this.context = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
    }

    private <T> Tuple2<UUID, T> newRDD(String name, UUID id, JavaRDD rdd) {
        rdd.setName(name);
        rddMap.put(id, new RDDContainer(id, name, rdd));
        return new Tuple2(id, rdd);
    }

    @Override
    public <T> JavaRDD<T> parallelize(List<T> items) {
        return context.parallelize(items);
    }

    @Override
    public <K, V> JavaPairRDD<K, V> parallelizePairs(List<Tuple2<K, V>> items) {
        return context.parallelizePairs(items);
    }

    @Override
    public <T> Tuple2<UUID, JavaRDD<T>> newRDD(T clazz, String name) {
        return newRDD(name, UUID.randomUUID(), context.emptyRDD());
    }

    @Override
    public <K, V> Tuple2<UUID, JavaPairRDD<K, V>> newPairRDD(K key, V value, String name) {
        JavaRDD rdd = context.emptyRDD();
        return newRDD(name, UUID.randomUUID(), rdd);
    }

    @Override
    public <T> JavaRDD<T> getRDD(UUID id) {
        if(rddMap.containsKey(id)) {
            return (JavaRDD<T>) rddMap.get(id).rdd;
        }
        throw new NoSuchElementException();
    }

    @Override
    public <K, V> JavaPairRDD<K, V> getPairRDD(UUID id) {
        if(rddMap.containsKey(id)) {
            return JavaPairRDD.fromJavaRDD((JavaRDD<Tuple2<K, V>>) rddMap.get(id).rdd);
        }
        throw new NoSuchElementException();
    }

    @Override
    public <T> void setRDD(UUID id, JavaRDD<T> rdd) {
        if(rddMap.containsKey(id)) {
            RDDContainer container = rddMap.get(id);
            container.rdd = rdd;
            rddMap.put(id, container);
            return;
        }
        throw new NoSuchElementException();
    }

    @Override
    public <K, V> void setPairRDD(UUID id, JavaPairRDD<K, V> rdd) {
        if(rddMap.containsKey(id)) {
            RDDContainer container = rddMap.get(id);
            container.rdd = rdd.rdd().toJavaRDD();
            rddMap.put(id, container);
            return;
        }
        throw new NoSuchElementException();
    }

    @Override
    public <T> void saveRDD(UUID id, String path) {
        getRDD(id).saveAsObjectFile(path);
    }

    @Override
    public <K, V> void savePairRDD(UUID id, String path) {
        getPairRDD(id).saveAsObjectFile(path);
    }

    @Override
    public <T> Tuple2<UUID, JavaRDD<T>> loadRDD(String name, String path) {
        return newRDD(name, UUID.randomUUID(), context.objectFile(path));
    }

    @Override
    public <K, V> Tuple2<UUID, JavaPairRDD<K, V>> loadPairRDD(String name, String path) {
        Tuple2<UUID, JavaRDD<Tuple2<K, V>>> t = newRDD(name, UUID.randomUUID(), context.objectFile(path));
        return new Tuple2<>(t._1(), JavaPairRDD.fromJavaRDD(t._2()));
    }
}
