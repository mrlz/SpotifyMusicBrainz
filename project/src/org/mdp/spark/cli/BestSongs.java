
/* Top 10 Results (number of times in Top 1, song#Artist):
 * (3630,Shape of You#Ed Sheeran)
 * (2342,Despacito - Remix#Luis Fonsi)
 * (1607,rockstar#Post Malone)
 * (1560,Despacito (Featuring Daddy Yankee)#Luis Fonsi)
 * (1180,Mi Gente#J Balvin)
 * (818,Felices los 4#Maluma)
 * (411,Criminal#Natti Natasha)
 * (299,Look What You Made Me Do#Taylor Swift)
 * (291,Me Rehúso#Danny Ocean)
 * (268,Sensualidad#Bad Bunny)
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
 * Get the number of times a  Songs#Artist was Top 1 in the Ranking of Spotify in 2017 
 * (independent of the country)
 */
public class BestSongs {
	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");
		
		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath1  outputPath");
			System.exit(0);
		}
		new BestSongs().run(args[0],args[1]);
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
				.setAppName(BestSongs.class.getName())
				.setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Load one RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> spotify = context.textFile(inputFilePath1);
		
		/*
		 * Filter per Top 1 and year 2017
		 */
		JavaRDD<String> lastYeartop1 = spotify.filter(
				line -> line.split(",")[0].equals("1") && line.split(",")[5].contains("2017")
				 
		);
		
		//Map: (title#artist)
		JavaRDD<String> canciones = lastYeartop1.map(
				line -> 	line.split(",")[1] + "#" + line.split(",")[2]
						
		);
		
		//MapToPair: (title#artist ,count of times song was Top 1)
		JavaPairRDD<String,Integer> countTitle = canciones.mapToPair(
				line ->new Tuple2<String,Integer> (line, 1)
		).reduceByKey((a, b) -> a + b);
		
		
		/* 
		 * Count is the key now, for sorting purpose (count, title#artist)
		 * Order by Count desc
		 */
		JavaPairRDD<Integer,String> totalKeyCount=countTitle.mapToPair(
				tupla -> new Tuple2<Integer,String>(
						tupla._2(), //count
						tupla._1())//title#artist
		).sortByKey(false);
		
		
		/*
		 * Write the output to local FS or HDFS
		 */
		totalKeyCount.saveAsTextFile(outputFilePath);
		
		
		
		context.close();
	}
}

