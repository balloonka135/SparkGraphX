package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;



public class Exercise_4 {



	public static class articlesID{
		public Long id;
		public String name;

		public articlesID(Long id, String name) {
			id=id;
			name=name;
		}
	}

	public static class edges implements Serializable {
		private Long src;
		private Long dst;

		public edges(Long src, Long dst) {
			this.src = src;
			this.dst = dst;
		}
	}


	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) throws FileNotFoundException {



		File file1 = new File("/Users/irinanazarchuk/Documents/uni/SDM/labs/SparkGraphXassignment/target/classes/wiki-vertices.txt");
		File file2 = new File("/Users/irinanazarchuk/Documents/uni/SDM/labs/SparkGraphXassignment/target/classes/wiki-edges.txt");

		Scanner input = new Scanner(file1);
		input.useDelimiter("\n");

		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build())
		});

		List<Row> vertices_list = new ArrayList<>();
		while(input.hasNext()) {
			String line = input.next();
			Long id = Long.parseLong(line.split("\t")[0]);
			String name = line.split("\t")[1];

			vertices_list.add(RowFactory.create(id, name));
		}

		Scanner input2= new Scanner(file2);
		input2.useDelimiter("\n");

		StructType edges_schema = new StructType(new StructField[]{
				new StructField("src", DataTypes.LongType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.LongType, true, new MetadataBuilder().build())
		});


		java.util.List<Row> edges_list = new ArrayList<Row>();
		while(input2.hasNext()) {
			String line = input2.next();
			Long src = Long.parseLong(line.split("\t")[0]);
			Long dst = Long.parseLong(line.split("\t")[1]);


			edges_list.add(RowFactory.create(src, dst));
		}


		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);
		Dataset<Row> v =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);


		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);
		Dataset<Row> e = sqlCtx.createDataFrame(edges_rdd, edges_schema);


		GraphFrame gf = new GraphFrame(v, e);
		System.out.println(gf);

		gf.edges().show();
		gf.vertices().show();




		Dataset pr =gf.pageRank().maxIter(20).resetProbability(0.85).run().vertices();
		pr.createOrReplaceTempView("pageranks");

		sqlCtx.sql("select * from pageranks order by pagerank desc").show(10);

	}

}
