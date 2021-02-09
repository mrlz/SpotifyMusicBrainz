/*Top 10 Canciones escuchadas en Navidad 2017 (Streams, song#Artist)
 (12914261,All I Want for Christmas Is You#Mariah Carey)
(10184364,Last Christmas#Wham!)
(7277220,Do They Know It's Christmas? - 1984 Version#Band Aid)
(7089139,rockstar#Post Malone)
(7045300,Rockin' Around The Christmas Tree - Single Version#Brenda Lee)
(6723604,It's Beginning To Look A Lot Like Christmas#Michael Bublé)
(6480482,It's the Most Wonderful Time of the Year#Andy Williams)
(6057027,Happy Xmas (War Is Over) - 2010 Digital Remaster#John Lennon)
(5989733,Havana#Camila Cabello)
(5868059,White Christmas#Bing Crosby)
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
 * Get the top songs streamed in Christmas, worldwide
 */
public class TopSongsNavidad {
	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");
		
		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath1  outputPath");
			System.exit(0);
		}
		new TopSongsNavidad().run(args[0],args[1]);
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
				.setAppName(TopSongsNavidad.class.getName())
				.setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Load one RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> spotify = context.textFile(inputFilePath1);
		
		//Filter Christmas 2017
		JavaRDD<String> navidad = spotify.filter(
				line -> line.split(",")[5].equals("2017-12-25")
				 
		);
		
		//Map: title#artist, streams
		JavaRDD<Tuple2<String,Integer>> canciones =  navidad.map(
				line -> new Tuple2<String,Integer> (
							line.split(",")[1] + "#" + line.split(",")[2], Integer.parseInt((line.split(",")[3]))
							
						)
		);
		
		//Total streams per song
		JavaPairRDD<String,Integer> totalStreams = canciones.mapToPair(
				tupla ->new Tuple2<String,Integer> (tupla._1(), tupla._2())
		).reduceByKey((a, b) -> a + b);
		
		/*
		 * Count is the key now, for sorting (desc) purpose.
		 * (count, title#artist)
		 */
		JavaPairRDD<Integer,String> totalSort=totalStreams.mapToPair(
				tupla -> new Tuple2<Integer,String>(
						tupla._2(), //count
						tupla._1())//title#artist
		).sortByKey(false);
		
		/*
		 * Write the output to local FS or HDFS
		 */
		totalSort.saveAsTextFile(outputFilePath);
		
		
		
		context.close();
	}
}
