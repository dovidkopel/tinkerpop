/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.io.File;
import java.util.Iterator;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkTest extends AbstractSparkTest {
    transient private final Logger logger = LoggerFactory.getLogger(getClass());

    @Test
    public void testSparkRDDPersistence() throws Exception {
        final String root = TestHelper.makeTestDataDirectory(SparkTest.class, "testSparkRDDPersistence");
        final String prefix = root + File.separator + "graphRDD-";
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        Spark.create(configuration);

        configuration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);

        for (int i = 0; i < 10; i++) {
            final String graphRDDName = Constants.getGraphLocation(prefix + i);
            assertEquals(i, Spark.getRDDs().size());
            configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, prefix + i);
            Graph graph = GraphFactory.open(configuration);
            graph.compute(SparkGraphComputer.class).persist(GraphComputer.Persist.VERTEX_PROPERTIES).program(PageRankVertexProgram.build().iterations(1).create(graph)).submit().get();
            assertNotNull(Spark.getRDD(graphRDDName));
            assertEquals(i + 1, Spark.getRDDs().size());
        }

        for (int i = 9; i >= 0; i--) {
            final String graphRDDName = Constants.getGraphLocation(prefix + i);
            assertEquals(i + 1, getPersistedRDDSize());
            assertEquals(i + 1, Spark.getRDDs().size());
            assertTrue(hasPersistedRDD(graphRDDName));
            Spark.removeRDD(graphRDDName);
            assertFalse(hasPersistedRDD(graphRDDName));
        }

        assertEquals(0, getPersistedRDDSize());
        assertEquals(0, Spark.getRDDs().size());
        Spark.close();
    }

    private static boolean hasPersistedRDD(final String name) {
        for (final RDD<?> rdd : JavaConversions.asJavaIterable(Spark.getContext().persistentRdds().values())) {
            if (null != rdd.name() && rdd.name().equals(name))
                return true;
        }
        return false;
    }

    private static int getPersistedRDDSize() {
        int counter = 0;
        for (final RDD<?> rdd : JavaConversions.asJavaIterable(Spark.getContext().persistentRdds().values())) {
            if (null != rdd.name())
                counter++;
        }
        return counter;
    }

    @Test
    public void testSparkGraph() throws Exception {
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("test").setMaster("local[4]");
        TransactionalSparkGraph graph = new TransactionalSparkGraph(sparkConf);
        SparkVertex v1 = graph.addVertex(T.label, "test1");
        logger.debug("{}", v1);

        Iterator it = graph.vertices(v1.id());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals(it.next(), v1);
        Assert.assertFalse(it.hasNext());

        SparkVertex v2 = graph.addVertex(T.label, "test2");
        logger.debug("{}", v2);

        it = graph.vertices(v2.id());
        Assert.assertTrue(it.hasNext());
        Assert.assertEquals(it.next(), v2);
        Assert.assertFalse(it.hasNext());

        SparkVertex v3 = graph.addVertex(T.label, "test3");
        logger.debug("{}", v3);

        Iterator<Vertex> it2 = graph.vertices();
        while(it2.hasNext()) {
            Vertex v = it2.next();
            logger.debug(v.id()+ ": "+v.label());
        }

        it2 = graph.vertices(0, 1);
        while(it.hasNext()) {
            Vertex v = it2.next();
            logger.debug(v.id()+ ": "+v.label());
        }

        SparkEdge e1 = v1.addEdge("loves", v2);
        logger.debug("{}", e1);

        SparkEdge e2 = v2.addEdge("hates", v3);
        logger.debug("{}", e2);


        System.out.println(((SparkVertex) graph.vertices(0).next()).vertices(Direction.OUT).next());
        GraphTraversal<Vertex, Vertex> tt = graph.traversal().V(0).out("loves");
        System.out.println(tt);
        System.out.println(tt.next());

        logger.debug("Edges: "+graph.traversal().E().count().next());
        logger.debug("Vertexes: "+graph.traversal().V().toList());
    }

}
