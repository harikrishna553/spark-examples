package com.sample.app.rdd;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.sample.app.demo.WordCountDemo;

public class RddFromFile {
	private static final String FILE_NAME = "/wordCount.txt";
	private static final String APP_NAME = "WordCount";

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			String filePath = WordCountDemo.class.getResource(FILE_NAME).getPath();

			JavaRDD<String> lines = javaSparkContext.textFile(filePath);

			JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

			Map<String, Long> wordCounts = words.countByValue();

			for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
				System.out.println(entry.getKey() + " : " + entry.getValue());
			}
		}

	}

}
