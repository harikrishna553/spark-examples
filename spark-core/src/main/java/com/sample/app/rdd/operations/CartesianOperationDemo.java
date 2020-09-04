package com.sample.app.rdd.operations;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CartesianOperationDemo {
	private static final String APP_NAME = SubtractOperationDemo.class.getName();

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		List<Integer> empIds = Arrays.asList(1, 2, 3, 4);
		List<String> empHobbies = Arrays.asList("Cricket", "Football");

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			JavaRDD<Integer> empIdsRdd = javaSparkContext.parallelize(empIds);
			JavaRDD<String> empHobbiesRdd = javaSparkContext.parallelize(empHobbies);

			JavaPairRDD<Integer, String> cartesianProduct = empIdsRdd.cartesian(empHobbiesRdd);

			cartesianProduct.collect().forEach(item -> {
				System.out.println(item._1 + "," + item._2);
			});

		}

	}

}
