package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.reflect.ConstructorUtils;
import org.apache.spark.SparkConf;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.SparkInterceptorStrategy;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.SparkSingleIterationStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by dkopel on 11/15/16.
 */
public class TransactionalSparkGraph<ID extends Long> extends AbstractSparkGraph<ID> {
    static {
        TraversalStrategies.GlobalCache.registerStrategies(
            TransactionalSparkGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                .addStrategies(
                    SparkSingleIterationStrategy.instance(),
                    SparkInterceptorStrategy.instance()
            )
        );
    }

    private Class<? extends GraphComputer> graphComputerClass = SparkGraphComputer.class;

    public TransactionalSparkGraph(SparkConf conf) {
        super(conf);
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
    public Iterator<Vertex> vertices(Collection<Object> vertexIds) {
        logger.debug("Getting vertices {}", vertexIds.toString());
        Iterator<SparkVertex> vs = filterElements(vertexIds, getVertexRDD());
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
            filterElements(edgeIds, getEdgeRDD()),
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
