package com.sample.app.actions;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CountDemo {
	private static final String APP_NAME = CountDemo.class.getName();

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		List<Integer> empIds = Arrays.asList(1, 2, 3, 4, 432, 215, 67, 908);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			JavaRDD<Integer> empIdsRdd = javaSparkContext.parallelize(empIds);

			long count = empIdsRdd.count();

			System.out.println("Total number of elements in RDD : " + count);
		}

	}

}
