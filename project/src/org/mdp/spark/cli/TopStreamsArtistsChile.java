/*Top 10 Results (total Streams in Chile, Artist):
 * (106552491,J Balvin)
 * (96540229,Ozuna)
 * (73839874,Maluma)
 * (67903946,Wisin)
 * (64491582,Ed Sheeran)
 * (57044470,Luis Fonsi)
 * (51057258,CNCO)
 * (50370290,Nicky Jam)
 * (49142340,Bad Bunny)
 * (45232429,Mon Laferte)
 */
package org.mdp.spark.cli;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;


/**
 * Get the Artists most streamed in Chile
 */
public class TopStreamsArtistsChile {
	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");
		
		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath1  outputPath");
			System.exit(0);
		}
		new TopStreamsArtistsChile().run(args[0],args[1]);
	}

	/**
	 * The task body
	 */
	public void run(String inputFilePath1, String outputFilePath) {
		/*
		 * This is the address of the Spark cluster. 
         * [*] means use all the cores available.
         * This can be overridden later when we call the application from the cluster.
		 * See {@see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls}.
		 */
		String master = "local[*]";

		/*
		 * Initialises a Spark context with the name of the application
		 *   and the (default) master settings.
		 */
		SparkConf conf = new SparkConf()
				.setAppName(TopStreamsArtistsChile.class.getName())
				.setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Load one RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> spotify = context.textFile(inputFilePath1);
		
		//Filter: Chile
		JavaRDD<String> chile =  spotify.filter(
				line -> line.split(",")[6].equals("cl")
											
		);
		//Map: artist, streams
		JavaRDD<Tuple2<String,Integer>> artists =  chile.map(
				line -> new Tuple2<String,Integer> (
							line.split(",")[2], Integer.parseInt(line.split(",")[3])
						)
		);
		//MapToPair: (artist, total Streams)
		JavaPairRDD<String,Integer> countTotalStreams = artists.mapToPair(
				tupla ->new Tuple2<String,Integer> (tupla._1(), tupla._2()) 
		).reduceByKey((a, b) -> a + b);
		
		//Sort by total streams desc
		JavaPairRDD<Integer,String> sortCount=countTotalStreams.mapToPair(
				tupla -> new Tuple2<Integer,String>(
						tupla._2(), //total streams
						tupla._1())//artist
		).sortByKey(false);
		
	
		/*
		 * Write the output to local FS or HDFS
		 */
		sortCount.saveAsTextFile(outputFilePath);
		
		
		
		context.close();
	}
}
