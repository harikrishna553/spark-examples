package com.sample.app.pair.rdd;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.sample.app.demo.WordCountDemo;

import scala.Tuple2;

public class WordCountUsingPairRddDemo {

	private static final String FILE_NAME = "/wordCount.txt";
	private static final String APP_NAME = "WordCountUsingPairRddDemo";

	private static class WordPairFuncton implements PairFunction<String, String, Integer> {

		private static final long serialVersionUID = 987531291L;

		@Override
		public Tuple2<String, Integer> call(String s) throws Exception {

			return new Tuple2<>(s, 1);
		}

	}

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			String filePath = WordCountDemo.class.getResource(FILE_NAME).getPath();

			JavaRDD<String> lines = javaSparkContext.textFile(filePath);

			JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

			JavaPairRDD<String, Integer> wordPairs = words.mapToPair(new WordPairFuncton());

			JavaPairRDD<String, Integer> wordCount = wordPairs.reduceByKey((x, y) -> x + y);

			wordCount.foreach(item -> {
				System.out.println(item._1 + " -> " + item._2);
			});

		}

	}

}
