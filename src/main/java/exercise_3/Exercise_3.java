package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

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

    private static class VProg extends AbstractFunction3<Long,Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
            // receives a message with a shortest path to it
            // if the message of max value, live current value
            // else min from the received

            if (message == Integer.MAX_VALUE) {             // superstep 0
                return vertexValue;
            } else {                                        // superstep > 0
                return Math.min(vertexValue,message);
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer,Integer>, Iterator<Tuple3<Object,Integer, ArrayList>>> implements Serializable {
        @Override
        public Iterator<Tuple3<Object, Integer, ArrayList>> apply(EdgeTriplet<Integer, Integer> triplet) {
            // if the path value (vertex value + edge value) to the next vertex
            // is smaller than the assigned value of that vertex, send it

            Tuple3<Object,Integer,ArrayList> sourceVertex = triplet.toTuple()._1();
            Tuple3<Object,Integer,ArrayList> dstVertex = triplet.toTuple()._2();
            Integer edgeValue = triplet.attr.intValue(); // edge weight value
            Integer sentValue = edgeValue + sourceVertex._2(); // value to be sent to the dest vertex

            // get arraylist from source vertex and add source vertex label to it
            ArrayList<String> pathVertices = sourceVertex._3(); // list of vertices on the path
            pathVertices.add(labels.get(sourceVertex._1()));

//
//            System.out.println("Sourse: " + sourceVertex._1 + " with value " + sourceVertex._2);
//            System.out.println("Dest: " + dstVertex._1 + " with value " + dstVertex._2);
//            System.out.println("Edge: " + edgeValue);

            if ((sentValue >= dstVertex._2()) || (sourceVertex._2() == Integer.MAX_VALUE)) {   // if source vertex value is bigger than dst vertex
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple3<Object,Integer, ArrayList>>().iterator()).asScala();
            } else {
                // ---------- send also array list of path vertices
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple3<Object,Integer, ArrayList>(triplet.dstId(),sentValue, pathVertices)).iterator()).asScala();
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

        List<Tuple3<Object,Integer, ArrayList>> vertices = Lists.newArrayList(
                new Tuple3<Object,Integer, ArrayList>(1l,0, new ArrayList<String>()),
                new Tuple3<Object,Integer, ArrayList>(2l,Integer.MAX_VALUE, new ArrayList<String>()),
                new Tuple3<Object,Integer, ArrayList>(3l,Integer.MAX_VALUE, new ArrayList<String>()),
                new Tuple3<Object,Integer, ArrayList>(4l,Integer.MAX_VALUE, new ArrayList<String>()),
                new Tuple3<Object,Integer, ArrayList>(5l,Integer.MAX_VALUE, new ArrayList<String>()),
                new Tuple3<Object,Integer, ArrayList>(6l,Integer.MAX_VALUE, new ArrayList<String>())
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

        JavaRDD<Tuple3<Object,Integer, ArrayList>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Integer,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(Integer.MAX_VALUE,
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(Integer.class))
                .vertices()
                .toJavaRDD()
                .foreach(v -> {
                    Tuple3<Object,Integer,ArrayList> vertex = (Tuple3<Object,Integer,ArrayList>)v;
                    // ---------- add new vertex attr "path" with an array of vertices and print it
                    System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1())+" is "+vertex._2());
                });
    }

}
