package org.apache.tinkerpop.gremlin.spark.structure.rdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;
import java.util.UUID;

/**
 * Created by dkopel on 11/17/16.
 */
public interface RDDManager {
    <T> JavaRDD<T> parallelize(List<T> items);
    <K, V> JavaPairRDD<K, V> parallelizePairs(List<Tuple2<K, V>> items);

    <T> Tuple2<UUID, JavaRDD<T>> newRDD(T clazz, String name);
    <K, V> Tuple2<UUID, JavaPairRDD<K, V>> newPairRDD(K key, V value, String name);

    <T> JavaRDD<T> getRDD(UUID id);
    <K, V> JavaPairRDD<K, V> getPairRDD(UUID id);

    <T> void setRDD(UUID id, JavaRDD<T> rdd);
    <K, V> void setPairRDD(UUID id, JavaPairRDD<K, V> rdd);

    <T> void saveRDD(UUID id, String path);
    <K, V> void savePairRDD(UUID id, String path);

    <T> Tuple2<UUID, JavaRDD<T>> loadRDD(String name, String path);
    <K, V> Tuple2<UUID, JavaPairRDD<K, V>> loadPairRDD(String name, String path);
}
