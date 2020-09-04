package com.sample.app.pair.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Iterables;

import scala.Tuple2;

public class WordCountDemo {
	private static final String APP_NAME = "WordCount";

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		List<String> wordsList = Arrays.asList("a", "an", "the", "an", "a", "cat");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			JavaPairRDD<String, Integer> wordPairsRdd = javaSparkContext.parallelize(wordsList)
					.mapToPair(word -> new Tuple2<>(word, 1));

			List<Tuple2<String, Integer>> wordCountUsingReduceOperation = wordPairsRdd.groupByKey()
					.mapValues(intIterable -> Iterables.size(intIterable)).collect();

			for (Tuple2<String, Integer> tuple : wordCountUsingReduceOperation) {
				System.out.println(tuple._1 + " -> " + tuple._2);
			}

		}

	}

}
