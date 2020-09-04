package com.sample.app.rdd.operations;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DistinctOperationDemo {
	private static final String APP_NAME = DistinctOperationDemo.class.getName();

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 5, 1, 2);
		List<Integer> list2 = Arrays.asList(2, 3, 6, 1, 2, 5, 6);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			JavaRDD<Integer> numbersRdd1 = javaSparkContext.parallelize(list1);
			JavaRDD<Integer> numbersRdd2 = javaSparkContext.parallelize(list2);

			JavaRDD<Integer> unionRdd = numbersRdd1.union(numbersRdd2);
			JavaRDD<Integer> unionAndDistinctRdd = numbersRdd1.union(numbersRdd2).distinct();

			System.out.println("Elements in unionRdd");
			unionRdd.collect().forEach(ele -> System.out.print(ele + " "));

			System.out.println("\nElements in unionAndDistinctRdd");
			unionAndDistinctRdd.collect().forEach(ele -> System.out.print(ele + " "));
		}

	}

}
