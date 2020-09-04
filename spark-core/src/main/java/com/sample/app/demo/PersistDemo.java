package com.sample.app.demo;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;

public class PersistDemo {
	private static final String APP_NAME = PersistDemo.class.getName();

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			JavaRDD<Integer> numbersRdd = javaSparkContext.parallelize(numbers);
			numbersRdd.persist(StorageLevels.MEMORY_ONLY);

			long sumOfElements = numbersRdd.reduce((a, b) -> a + b);
			long count = numbersRdd.count();

			System.out.println("Sum Of Elements : " + sumOfElements);
			System.out.println("Total elements : " + count);

		}

	}

}
