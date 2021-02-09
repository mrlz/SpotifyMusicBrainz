package org.mdp.spark.cli;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * WordCountTask class, we will call this class with the test WordCountTest.
 */
public class WordCountTaskJ7 implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -353277666982984630L;

	/**
	 * This is the entry point when the task is called from command line with spark-submit.sh.
	 * See {@see http://spark.apache.org/docs/latest/submitting-applications.html}
	 */
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/");
		if(args.length != 2) {
			System.err.println("Usage arguments: inputPath outputPath");
			System.exit(0);
		}
		new WordCountTaskJ7().run(args[0],args[1]);
	}

	/**
	 * The task body
	 */
	@SuppressWarnings("serial")
	public void run(String inputFilePath, String outputFilePath) {
		/*
		 * This is the address of the Spark cluster. We will call the task from WordCountTest and we
		 * use a local standalone cluster. [*] means use all the cores available.
		 * See {@see http://spark.apache.org/docs/latest/submitting-applications.html#master-urls}.
		 */
		String master = "local[*]";

		/*
		 * Initialises a Spark context.
		 */
		SparkConf conf = new SparkConf()
				.setAppName(WordCountTaskJ7.class.getName())
				.setMaster(master);
		JavaSparkContext context = new JavaSparkContext(conf);

		/*
		 * Performs a work count sequence of tasks and prints the output with a logger.
		 */
		JavaRDD<String> inputRDD = context.textFile(inputFilePath);
		
		/*
		 * Map lines to words
		 */
		JavaRDD<String> wordRDD = inputRDD.flatMap(
					new FlatMapFunction<String,String>() {
						@Override
						public Iterator<String> call(String text) throws Exception {
							return Arrays.<String>asList(text.split(" ")).iterator();
						}
					}
				);
		
		/*
		 * Map (word) -> (word,1)
		 */
		JavaPairRDD<String,Integer> wordOneRDD = wordRDD.mapToPair(
					new PairFunction<String,String,Integer>() {
						@Override
						public Tuple2<String,Integer> call(String text) throws Exception {
							return new Tuple2<String,Integer>(text,1);
						}
					}				
				);
		
		/*
		 * Sum values over (word,1) for each word key
		 */
		JavaPairRDD<String,Integer> wordCount = wordOneRDD.reduceByKey(
					new Function2<Integer,Integer,Integer>() {
						@Override
						public Integer call(Integer a,Integer b) throws Exception {
							return a+b;
						}
					}	
				);
		wordCount.saveAsTextFile(outputFilePath);
		
		context.close();
	}
}
