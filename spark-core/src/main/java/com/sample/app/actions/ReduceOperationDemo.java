package com.sample.app.actions;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReduceOperationDemo {
	private static final String APP_NAME = ReduceOperationDemo.class.getName();

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			JavaRDD<Integer> numbersRdd = javaSparkContext.parallelize(numbers);

			long sumOfElements = numbersRdd.reduce((a, b) -> a + b);
			long productOfElements = numbersRdd.reduce((a, b) -> a * b);

			System.out.println("Sum Of Elements : " + sumOfElements);
			System.out.println("Product of Elements : " + productOfElements);

		}

	}

}
