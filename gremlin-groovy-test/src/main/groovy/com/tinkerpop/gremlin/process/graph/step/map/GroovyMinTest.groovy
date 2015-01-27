package com.tinkerpop.gremlin.process.graph.step.map

import com.tinkerpop.gremlin.process.Traversal
import com.tinkerpop.gremlin.process.graph.step.ComputerTestHelper
import com.tinkerpop.gremlin.structure.Vertex

import static com.tinkerpop.gremlin.process.graph.AnonymousGraphTraversal.Tokens.__

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class GroovyMinTest {

    public static class StandardTest extends MinTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_min() {
            g.V.age.min
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min() {
            g.V.repeat(__.both).times(5).age.min
        }

    }

    public static class ComputerTest extends MinTest {

        @Override
        public Traversal<Vertex, Integer> get_g_V_age_min() {
            ComputerTestHelper.compute("g.V.age.min", g)
        }

        @Override
        public Traversal<Vertex, Integer> get_g_V_repeatXbothX_timesX5X_age_min() {
            ComputerTestHelper.compute("g.V.repeat(__.both).times(5).age.min", g)
        }
    }
}
