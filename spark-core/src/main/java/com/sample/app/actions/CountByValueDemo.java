package com.sample.app.actions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class CountByValueDemo {
	private static final String APP_NAME = CountByValueDemo.class.getName();

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		List<Integer> empIds = Arrays.asList(23, 45, 23, 41, 45, 32, 1);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			JavaRDD<Integer> empIdsRdd = javaSparkContext.parallelize(empIds);

			Map<Integer, Long> idsMap = empIdsRdd.countByValue();

			idsMap.entrySet().stream().forEach(item -> {
				System.out.println(item.getKey() + " repeated " + item.getValue() + " times");
			});
		}

	}

}
