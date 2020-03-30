package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Array;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import shapeless.Tuple;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;


public class Exercise_3 {

    public static Map<Long, String> labels = ImmutableMap.<Long, String>builder()
            .put(1l, "A")
            .put(2l, "B")
            .put(3l, "C")
            .put(4l, "D")
            .put(5l, "E")
            .put(6l, "F")
            .build();

    private static class VProg extends AbstractFunction3<Long, Tuple2<Integer, ArrayList>, Tuple2<Integer, ArrayList>, Tuple2<Integer, ArrayList>> implements Serializable {
        @Override
        public Tuple2<Integer, ArrayList> apply(Long vertexID, Tuple2<Integer, ArrayList> vertexValue, Tuple2<Integer, ArrayList> message) {

            if (message._1 == Integer.MAX_VALUE) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
                if (vertexValue._1 == Math.min(vertexValue._1, message._1)) {
                    return vertexValue;
                } else {
                    return message;
                }
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer, ArrayList>,Integer>, Iterator<Tuple2<Object, Tuple2<Integer, ArrayList>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer, ArrayList>>> apply(EdgeTriplet<Tuple2<Integer, ArrayList>, Integer> triplet) {
            Tuple2<Object, Tuple2<Integer, ArrayList>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object, Tuple2<Integer, ArrayList>> dstVertex = triplet.toTuple()._2();
            Integer edgeValue = triplet.attr.intValue(); // edge weight value
            Integer sentValue = edgeValue + sourceVertex._2._1; // value to be sent to the dest vertex

            ArrayList<String> pathVertices = sourceVertex._2._2; // list of vertices on the path
            pathVertices.add(labels.get(dstVertex._1()));

            if ((sentValue >= dstVertex._2()._1) || (sourceVertex._2()._1 == Integer.MAX_VALUE)) {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object, Tuple2<Integer, ArrayList>>>().iterator()).asScala();
            } else {
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object, Tuple2<Integer, ArrayList>>(triplet.dstId(), new Tuple2(sentValue, pathVertices))).iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            return Math.min(o,o2);
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Tuple2<Object, Tuple2<Integer, ArrayList>>> vertices = Lists.newArrayList(
                new Tuple2<Object, Tuple2<Integer, ArrayList>>(1l, new Tuple2(0, new ArrayList<String>(Arrays.asList("A")))),
                new Tuple2<Object, Tuple2<Integer, ArrayList>>(2l, new Tuple2(Integer.MAX_VALUE, new ArrayList<String>())),
                new Tuple2<Object, Tuple2<Integer, ArrayList>>(3l, new Tuple2(Integer.MAX_VALUE, new ArrayList<String>())),
                new Tuple2<Object, Tuple2<Integer, ArrayList>>(4l, new Tuple2(Integer.MAX_VALUE, new ArrayList<String>())),
                new Tuple2<Object, Tuple2<Integer, ArrayList>>(5l, new Tuple2(Integer.MAX_VALUE, new ArrayList<String>())),
                new Tuple2<Object, Tuple2<Integer, ArrayList>>(6l, new Tuple2(Integer.MAX_VALUE, new ArrayList<String>()))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, Tuple2<Integer, ArrayList>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2<Integer, ArrayList>, Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Tuple2<Integer, ArrayList>(1, new ArrayList<String>()), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                ClassTag$.MODULE$.apply(Tuple2.class), ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new Tuple2(Integer.MAX_VALUE, new ArrayList<String>()),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD()
                .foreach(v -> {
                    Tuple2<Object, Tuple2<Integer, ArrayList>> vertex = (Tuple2<Object, Tuple2<Integer, ArrayList>>)v;
                    System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1())+" is [" + String.join(", ", vertex._2()._2) + "] with cost " + vertex._2()._1);
                });
    }

}
