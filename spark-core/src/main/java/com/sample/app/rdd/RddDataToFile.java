package com.sample.app.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RddDataToFile {
	private static final String APP_NAME = RddDataToFile.class.getName();

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			List<String> list = Arrays.asList("One", "Two", "Three", "Four");

			JavaRDD<String> textRdd = javaSparkContext.parallelize(list);

			textRdd.saveAsTextFile("out");

		}

	}

}
