package com.sample.app.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FlatMapDemo {
	private static final String APP_NAME = FlatMapDemo.class.getName();

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		List<Integer> primeNumbers = Arrays.asList(2, 3, 5, 7);
		List<Integer> evenNumbers = Arrays.asList(2, 4, 6, 8);
		List<Integer> oddNumbers = Arrays.asList(1, 3, 5, 7);

		List<List<Integer>> allTypeOfNumbers = Arrays.asList(primeNumbers, evenNumbers, oddNumbers);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			JavaRDD<List<Integer>> numbersRdd = javaSparkContext.parallelize(allTypeOfNumbers);

			JavaRDD<Integer> flattenedNumbers = numbersRdd.flatMap(list -> list.iterator());

			flattenedNumbers.collect().forEach(ele -> System.out.print(ele + " "));

		}

	}

}
