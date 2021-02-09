/*Canciones más reproducidas en Chile el 18 de Septiembre de 2017 (total Streams, song#Artist):
 * (138672,Mi Gente#J Balvin)
 * (118771,Me Rehúso#Danny Ocean)
 * (110358,Bonita#J Balvin)
 * (107646,Mayores#Becky G)
 * (99992,Se Preparó#Ozuna)
 * (98507,Báilame - Remix#Nacho)
 * (96890,Escápate Conmigo#Wisin)
 * (95008,Felices los 4#Maluma)
 * (91310,Bella y Sensual#Romeo Santos)
 * (84316,Una Lady Como Tú#Manuel Turizo)
 * (80874,El Guatón Loyola#Hugo Lagos)
 * (79017,La Consentida#Hugo Lagos)
 * (78286,Ahora Dice#Chris Jeday)
 * (75981,Explícale#Yandel)
 * (74955,Hey DJ#CNCO)
 * (71264,Escápate Conmigo - Remix#Wisin)
 * (68423,Krippy Kush#Farruko)
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
 * Get the most streamed songs in September 18, 2017 in Chile  
 */
public class TopCancionesChileSeptiembre {
	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");
		
		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath1  outputPath");
			System.exit(0);
		}
		new TopCancionesChileSeptiembre().run(args[0],args[1]);
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
				.setAppName(TopCancionesChileSeptiembre.class.getName())
				.setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Load one RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> spotify = context.textFile(inputFilePath1);
		

		
		//Filter per country = Chile and date = 2017-09-18
		JavaRDD<String> chile18Sept =  spotify.filter(
				line -> line.split(",")[6].equals("cl") &&  line.split(",")[5].equals("2017-09-18")
											
		);
		
		
		//Map: title#artist, streams
		JavaRDD<Tuple2<String,Integer>> streams =  chile18Sept.map(
				line -> new Tuple2<String,Integer> (
							line.split(",")[1] + "#" + line.split(",")[2], Integer.parseInt((line.split(",")[3]))
							
						)
		);
		
		//MapToPair: title#artist, total streams
		JavaPairRDD<String,Integer> totalStreams = streams.mapToPair(
				tupla -> new Tuple2<String,Integer> (tupla._1(), tupla._2()) 
		).reduceByKey((a, b) -> a + b);
		
		/*
		 * Count is the key now, for sorting (desc) purpose.
		 * (count, title#artist)
		 */
		JavaPairRDD<Integer,String> totalKeyCount=totalStreams.mapToPair(
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
