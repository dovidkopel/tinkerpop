package org.apache.tinkerpop.gremlin.spark.process.computer;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.*;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.spark.structure.SparkGraph;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Created by dkopel on 11/15/16.
 */
public class SparkGraphComputeComputer implements GraphComputer {
    protected final SparkGraph graph;
    protected final Set<MapReduce> mapReducers = new HashSet<>();
    protected VertexProgram<Object> vertexProgram;
    protected int workers = 1;

    protected ResultGraph resultGraph = null;
    protected Persist persist = null;

    protected GraphFilter graphFilter = new GraphFilter();

    public SparkGraphComputeComputer(SparkGraph graph) {
        this.graph = graph;
    }

    @Override
    public GraphComputer result(ResultGraph resultGraph) {
        this.resultGraph = resultGraph;
        return this;
    }

    @Override
    public GraphComputer persist(Persist persist) {
        this.persist = persist;
        return this;
    }

    @Override
    public GraphComputer program(VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }

    @Override
    public GraphComputer mapReduce(MapReduce mapReduce) {
        this.mapReducers.add(mapReduce);
        return this;
    }

    @Override
    public GraphComputer workers(int workers) {
        this.workers = workers;
        return this;
    }

    @Override
    public GraphComputer vertices(Traversal<Vertex, Vertex> vertexFilter) throws IllegalArgumentException {
        this.graphFilter.setVertexFilter(vertexFilter);
        return this;
    }

    @Override
    public GraphComputer edges(Traversal<Vertex, Edge> edgeFilter) throws IllegalArgumentException {
        this.graphFilter.setEdgeFilter(edgeFilter);
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        BaseConfiguration conf = new BaseConfiguration();
        TraversalVertexProgram vp = VertexProgram.createVertexProgram(graph, conf);

        final Traversal.Admin<Vertex, Object> traversal = (Traversal.Admin) vp.getTraversal().getPure().clone();

        return null;
    }
}
