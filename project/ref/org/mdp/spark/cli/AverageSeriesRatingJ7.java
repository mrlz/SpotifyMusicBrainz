package org.mdp.spark.cli;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

/**
 * Get the average ratings of TV series from IMDb.
 * 
 * This is the Java 7 version with anonymous classes
 */
public class AverageSeriesRatingJ7 implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5424808787176133969L;

	/**
	 * This will be called by spark
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");
		
		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath outputPath");
			System.exit(0);
		}
		new AverageSeriesRatingJ7().run(args[0],args[1]);
	}

	/**
	 * The task body
	 */
	@SuppressWarnings("serial")
	public void run(String inputFilePath, String outputFilePath) {
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
				.setAppName(AverageSeriesRatingJ7.class.getName())
				.setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Load the first RDD from the input location (a local file, HDFS file, etc.)
		 */
		JavaRDD<String> inputRDD = context.textFile(inputFilePath);
		
		/*
		 * Here we filter lines that are not TV series or where no episode name is given
		 */
		JavaRDD<String> tvSeries = inputRDD.filter(
				new Function<String,Boolean>() {
					@Override
					public Boolean call(String s){
						String[] split = s.split("\t");
						return split[6].equals("TV_SERIES") && !split[7].equals("null");
					}
				}
		);
		
		/*
		 * We create a tuple (series,episode,rating)
		 * where series is the key (name+"#"+year)
		 */
		JavaRDD<Tuple3<String,String,Double>> seriesEpisodeRating = tvSeries.map(
				new Function<String,Tuple3<String,String,Double>>() {
					public Tuple3<String,String,Double> call(String s) throws Exception {
						String[] split = s.split("\t");
						String series = split[3] + "#" + split[4];
						String episode = split[7]; 
						Double rating = Double.parseDouble(split[2]);
						return new Tuple3<String,String,Double>(series,episode,rating);
					}
				}
		);
		
		/*
		 * Now we start to compute the average rating per series.
		 * 
		 * We don't care about the episode name for now so to start with, 
		 * from tuples (series,episode,rating)
		 * we will produce a map: (series,rating)
		 * 
		 * (We could have done this directly from tvSeries, 
		 *   except seriesEpisodeRating will be reused later)
		 */
		JavaPairRDD<String,Double> seriesToEpisodeRating = seriesEpisodeRating.mapToPair(
				new PairFunction<Tuple3<String,String,Double>,String,Double>() {
					public Tuple2<String, Double> call(Tuple3<String,String,Double> sER) throws Exception {
						return new Tuple2<String, Double>(sER._1(), sER._3());
					}
				}
		);
		
		/*
		 * To compute the average rating for each series, the idea is to
		 * maintain the following tuples:
		 * 
		 * (series,(count,sum))
		 * 
		 * Where series is the series identifier, 
		 *   count is the number of episode ratings thus far
		 *   sum is the sum of episode ratings thus far
		 *
		 * Base value: (0,0)
		 *
		 * To combine (count,sum) | rating:
		 *   (count+1,sum+rating)
		 *   
		 * To reduce (count1,sum1) | (count2,sum2)
		 *   (count1+count2,sum1+sum2)
		 */
		JavaPairRDD<String,Tuple2<Double,Integer>> seriesToSumCountRating = seriesToEpisodeRating.aggregateByKey(
				new Tuple2<Double,Integer>(0d,0), // base value
				new Function2<Tuple2<Double,Integer>,Double,Tuple2<Double,Integer>>() { // combine function
					@Override
					public Tuple2<Double, Integer> call(Tuple2<Double, Integer> a, Double b)
							throws Exception {
						return new Tuple2<Double,Integer>(a._1+b, a._2+1);
					}
				},
				new Function2<Tuple2<Double,Integer>,Tuple2<Double,Integer>,Tuple2<Double,Integer>>() { // reduce function
					@Override
					public Tuple2<Double,Integer> call(Tuple2<Double,Integer> a, Tuple2<Double,Integer> b) throws Exception {
						return new Tuple2<Double,Integer>(a._1+b._1, a._2+b._2);
					}
				}
		);
		
		/*
		 * Given final values for:
		 * 
		 * (series,(count,sum))
		 * 
		 * Create the average:
		 * 
		 * (series,count/sum)
		 */
		JavaPairRDD<String,Double> seriesToAvgRating = seriesToSumCountRating.mapToPair(
				new PairFunction<Tuple2<String,Tuple2<Double,Integer>>,String,Double>() {
					@Override
					public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double,Integer>> a) throws Exception {
						return new Tuple2<String,Double>(a._1,a._2._1/a._2._2);
					}
					
				}
		);
		
		/*
		 * Write the output to local FS or HDFS
		 */
		seriesToAvgRating.saveAsTextFile(outputFilePath);
		
		context.close();
	}
}
