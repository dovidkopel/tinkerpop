package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by dkopel on 11/15/16.
 */
public class DistributedSparkGraph<ID extends Long> extends AbstractSparkGraph<ID> {
    private final AtomicLong currentId = new AtomicLong(-1L);

    public DistributedSparkGraph(SparkConf conf) {
        super(conf);
    }

    @Override
    public ID nextId() {
        return (ID) ((Long) currentId.incrementAndGet());
    }

    @Override
    public ID toId(Object id) {
        if(clazz.isInstance(id)) {
            return (ID) id;
        } else {
            return (ID) Long.valueOf(id.toString());
        }
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        try {
            return graphComputerClass.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Collection<Object> vertexIds) {
        logger.debug("Getting vertices {}", vertexIds.toString());
        Iterator<SparkVertex> vs = filterElements(vertexIds, SparkRDD.VERTEX);
        Iterator<Vertex> vv = IteratorUtils.map(
            vs,
            v -> (Vertex) v
        );
        return vv;
    }

    @Override
    public Iterator<Edge> edges(Collection<Object> edgeIds) {
        logger.debug("Getting edges {}", edgeIds.toString());
        return IteratorUtils.map(
            filterElements(edgeIds, SparkRDD.EDGE),
            e -> (Edge) e
        );
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return vertices(Arrays.asList(vertexIds));
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return edges(Arrays.asList(edgeIds));
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
