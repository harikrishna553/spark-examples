package com.sample.app.rdd;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RddFromList {
	private static final String APP_NAME = "RDDFromList";

	private static List<Integer> getList(int nNumbers) {
		List<Integer> list = new ArrayList<>();

		for (int i = 0; i < nNumbers; i++) {
			list.add(i);
		}

		return list;
	}

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			List<Integer> list = getList(40);

			JavaRDD<Integer> integersRdd = javaSparkContext.parallelize(list);

			JavaRDD<Integer> evenIntegers = integersRdd.filter(i -> i % 2 == 0);
			
			evenIntegers.collect().forEach(System.out::println);
			
		}

	}

}
