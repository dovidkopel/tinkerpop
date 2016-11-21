package org.apache.tinkerpop.gremlin.spark.structure;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.tinkerpop.gremlin.spark.GraphDriver;
import org.apache.tinkerpop.gremlin.spark.structure.rdd.DataService;
import org.apache.tinkerpop.gremlin.spark.structure.rdd.GraphRDDManager;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.reflect.AnnotatedType;
import java.lang.reflect.TypeVariable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by dkopel on 11/16/16.
 */
public abstract class AbstractSparkGraph<ID extends Long> implements SparkGraph<ID> {
    transient final Logger logger = LoggerFactory.getLogger(getClass());

    private final DataService dataService;
    private final UUID uuid;
    final Class<ID> clazz;

    public AbstractSparkGraph(SparkConf sparkConf) {
        this.uuid = GraphDriver.INSTANCE.newgraph(this);
        Class c = Object.class;
        for(TypeVariable t : this.getClass().getTypeParameters()) {
            for(AnnotatedType b : t.getAnnotatedBounds()) {
                c = (Class) b.getType();
            }
        }
        this.clazz = c;
        this.dataService = new DataService(new GraphRDDManager(sparkConf), clazz);
    }

    protected abstract ID nextId();
    protected abstract ID toId(Object id);

    @Override
    public UUID getUUID() {
        return uuid;
    }

    protected <T extends SparkElement> Iterator<T> filterElements(Collection<Object> elementIds, SparkRDD rddType) {
        return filterElements(elementIds, dataService.getRDD(rddType));
    }

    protected <T extends SparkElement> Iterator<T> filterElements(Collection<Object> elementIds, JavaPairRDD<ID, T> rdd) {
        if(elementIds != null && elementIds.size() > 0) {
            Set<ID> ids = elementIds.stream().map(id -> toId(id)).collect(Collectors.toSet());
            logger.debug("Filtering elements ids: {}", ids);
            return rdd
                .filter((t) -> Sets.newHashSet(ids).contains(t._1()))
                .map(t -> t._2())
                .toLocalIterator();
        } else {
            return rdd
                .map(t -> t._2())
                .toLocalIterator();
        }
    }

    protected Tuple2<ID, String> prepareElement(Class<? extends Element> clazz, Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        ID id;
        if(null == idValue || idValue == Optional.empty()) {
            id = nextId();
        } else {
            id = toId(idValue);
        }
        logger.debug("prepare element id: {}", id);

        final String label = ElementHelper.getLabelValue(keyValues).orElse(defaultLabel(clazz));
        return new Tuple2(id, label);
    }

    @Override
    public Iterator<? extends SparkElement> getRDD(ID... ids) {
        return dataService.getRDD(Lists.newArrayList(ids));
    }

    @Override
    public SparkVertex addVertex(Object... keyValues) {
        Tuple2<ID, String> t = prepareElement(Vertex.class, keyValues);
        SparkVertex vertex = new SparkVertex(t._1(), t._2(), this);
        dataService.addVertexes(vertex);
        return vertex;
    }

    @Override
    public SparkEdge addEdge(SparkVertex in, SparkVertex out, Object... keyValues) {
        Tuple2<ID, String> t = prepareElement(Edge.class, keyValues);
        SparkEdge edge = new SparkEdge(t._1(), out, t._2(), in);
        in._addEdge(Direction.IN, t._2(), t._1());
        out._addEdge(Direction.OUT, t._2(), t._1());
        addToRDD(Lists.newArrayList(edge, in, out));
        return edge;
    }

    public <T extends AbstractSparkEntity> void removeFromRDD(Collection<T> elements) {
        dataService.removeVertexes(elements.stream()
            .filter(e -> e instanceof SparkVertex)
            .map(e -> (SparkVertex) e)
            .collect(Collectors.toList()));

        dataService.removeEdges(elements.stream()
            .filter(e -> e instanceof SparkEdge)
            .map(e -> (SparkEdge) e)
            .collect(Collectors.toList()));

        dataService.removeProperties(elements.stream()
            .filter(e -> e instanceof SparkProperty)
            .map(e -> (SparkProperty) e)
            .collect(Collectors.toList()));
    }

    public <T extends AbstractSparkEntity> void addToRDD(Collection<T> elements) {
        dataService.addVertexes(elements.stream()
            .filter(e -> e instanceof SparkVertex)
            .map(e -> (SparkVertex) e)
            .collect(Collectors.toList()));

        dataService.addEdges(elements.stream()
            .filter(e -> e instanceof SparkEdge)
            .map(e -> (SparkEdge) e)
            .collect(Collectors.toList()));

        dataService.addProperties(elements.stream()
            .filter(e -> e instanceof SparkProperty)
            .map(e -> (SparkProperty) e)
            .collect(Collectors.toList()));
    }

}
