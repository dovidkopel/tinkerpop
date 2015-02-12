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
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.process.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.strategy.GraphStrategy;
import org.apache.tinkerpop.gremlin.structure.strategy.StrategyElement;
import org.apache.tinkerpop.gremlin.structure.strategy.StrategyProperty;
import org.apache.tinkerpop.gremlin.structure.strategy.StrategyVariables;
import org.apache.tinkerpop.gremlin.util.function.FunctionUtils;
import org.javatuples.Pair;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;


/**
 * A collection of helpful methods for creating standard {@link Object#toString()} representations of graph-related
 * objects.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class StringFactory {

    private static final String V = "v";
    private static final String E = "e";
    private static final String P = "p";
    private static final String VP = "vp";
    private static final String L_BRACKET = "[";
    private static final String R_BRACKET = "]";
    private static final String COMMA_SPACE = ", ";
    private static final String COLON = ":";
    private static final String EMPTY_MAP = "{}";
    private static final String DOTS = "...";
    private static final String DASH = "-";
    private static final String ARROW = "->";
    private static final String STAR = "*";
    private static final String EMPTY_PROPERTY = "p[empty]";
    private static final String EMPTY_VERTEX_PROPERTY = "vp[empty]";
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    private static final String featuresStartWith = "supports";
    private static final int prefixLength = featuresStartWith.length();

    private StringFactory() {
    }

    /**
     * Construct the representation for a {@link org.apache.tinkerpop.gremlin.structure.Vertex}.
     */
    public static String vertexString(final Vertex vertex) {
        return V + L_BRACKET + vertex.id() + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link org.apache.tinkerpop.gremlin.structure.Edge}.
     */
    public static String edgeString(final Edge edge) {
        final Vertex inV = edge.iterators().vertexIterator(Direction.IN).next();
        final Vertex outV = edge.iterators().vertexIterator(Direction.OUT).next();
        return E + L_BRACKET + edge.id() + R_BRACKET + L_BRACKET + outV.id() + DASH + edge.label() + ARROW + inV.id() + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link org.apache.tinkerpop.gremlin.structure.Property} or {@link org.apache.tinkerpop.gremlin.structure.VertexProperty}.
     */
    public static String propertyString(final Property property) {
        if (property instanceof VertexProperty) {
            if (!property.isPresent()) return EMPTY_VERTEX_PROPERTY;
            final String valueString = String.valueOf(property.value());
            return VP + L_BRACKET + property.key() + ARROW + valueString.substring(0, Math.min(valueString.length(), 20)) + R_BRACKET;
        } else {
            if (!property.isPresent()) return EMPTY_PROPERTY;
            final String valueString = String.valueOf(property.value());
            return P + L_BRACKET + property.key() + ARROW + valueString.substring(0, Math.min(valueString.length(), 20)) + R_BRACKET;
        }
    }

    /**
     * Construct the representation for a {@link org.apache.tinkerpop.gremlin.structure.Graph}.
     *
     * @param internalString a mapper {@link String} that appends to the end of the standard representation
     */
    public static String graphString(final Graph graph, final String internalString) {
        return graph.getClass().getSimpleName().toLowerCase() + L_BRACKET + internalString + R_BRACKET;
    }

    /**
     * Construct the representation for a {@link org.apache.tinkerpop.gremlin.structure.strategy.GraphStrategy}.
     */

    public static String graphStrategyString(final GraphStrategy graphStrategy) {
        return graphStrategy.getClass().getSimpleName().toLowerCase();
    }

    public static String graphStrategyString(final GraphStrategy graphStrategy, final Graph graph) {
        return graphStrategy.getClass().getSimpleName().toLowerCase() + L_BRACKET + graph.toString() + R_BRACKET;
    }

    public static String graphStrategyElementString(final StrategyElement element) {
        return element.getBaseElement() + STAR;
    }

    public static String graphStrategyPropertyString(final StrategyProperty property) {
        return property.getBaseProperty() + STAR;
    }

    public static String graphStrategyVariables(final StrategyVariables variables) {
        return variables.getBaseVariables() + STAR;
    }

    public static String graphVariablesString(final Graph.Variables variables) {
        return "variables" + L_BRACKET + "size:" + variables.keys().size() + R_BRACKET;
    }

    public static String memoryString(final Memory memory) {
        return "memory" + L_BRACKET + "size:" + memory.keys().size() + R_BRACKET;
    }

    public static String computeResultString(final ComputerResult computerResult) {
        return "result" + L_BRACKET + computerResult.graph() + ',' + computerResult.memory() + R_BRACKET;
    }

    public static String graphComputerString(final GraphComputer graphComputer) {
        return graphComputer.getClass().getSimpleName().toLowerCase();
    }

    public static String featureString(final Graph.Features features) {
        final StringBuilder sb = new StringBuilder("FEATURES");
        final Predicate<Method> supportMethods = (m) -> m.getModifiers() == Modifier.PUBLIC && m.getName().startsWith(featuresStartWith) && !m.getName().equals(featuresStartWith);
        sb.append(LINE_SEPARATOR);

        Stream.of(Pair.with(Graph.Features.GraphFeatures.class, features.graph()),
                Pair.with(Graph.Features.VariableFeatures.class, features.graph().variables()),
                Pair.with(Graph.Features.VertexFeatures.class, features.vertex()),
                Pair.with(Graph.Features.VertexPropertyFeatures.class, features.vertex().properties()),
                Pair.with(Graph.Features.EdgeFeatures.class, features.edge()),
                Pair.with(Graph.Features.EdgePropertyFeatures.class, features.edge().properties())).forEach(p -> {
            printFeatureTitle(p.getValue0(), sb);
            Stream.of(p.getValue0().getMethods())
                    .filter(supportMethods)
                    .map(createTransform(p.getValue1()))
                    .forEach(sb::append);
        });

        return sb.toString();
    }

    public static String traversalSideEffectsString(final TraversalSideEffects traversalSideEffects) {
        return "sideEffects" + L_BRACKET + "size:" + traversalSideEffects.keys().size() + R_BRACKET;
    }

    public static String traversalStrategiesString(final TraversalStrategies traversalStrategies) {
        return "strategies" + traversalStrategies.toList();
    }

    public static String traversalStrategyString(final TraversalStrategy traversalStrategy) {
        return traversalStrategy.getClass().getSimpleName();
    }

    public static String vertexProgramString(final VertexProgram vertexProgram, final String internalString) {
        return vertexProgram.getClass().getSimpleName() + L_BRACKET + internalString + R_BRACKET;
    }

    public static String vertexProgramString(final VertexProgram vertexProgram) {
        return vertexProgram.getClass().getSimpleName();
    }

    public static String mapReduceString(final MapReduce mapReduce, final String internalString) {
        return mapReduce.getClass().getSimpleName() + L_BRACKET + internalString + R_BRACKET;
    }

    public static String mapReduceString(final MapReduce mapReduce) {
        return mapReduce.getClass().getSimpleName();
    }

    private static Function<Method, String> createTransform(final Graph.Features.FeatureSet features) {
        return FunctionUtils.wrapFunction((m) -> ">-- " + m.getName().substring(prefixLength) + ": " + m.invoke(features).toString() + LINE_SEPARATOR);
    }

    private static void printFeatureTitle(final Class<? extends Graph.Features.FeatureSet> featureClass, final StringBuilder sb) {
        sb.append("> ");
        sb.append(featureClass.getSimpleName());
        sb.append(LINE_SEPARATOR);
    }
}