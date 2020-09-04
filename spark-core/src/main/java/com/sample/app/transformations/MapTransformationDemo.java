package com.sample.app.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MapTransformationDemo {
	private static final String APP_NAME = FilterTransformationDemo1.class.getName();

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			List<String> list = Arrays.asList("One", "Two", "Three", "Four");

			JavaRDD<String> textRdd = javaSparkContext.parallelize(list);

			JavaRDD<String> stringUpperCaseRdd = textRdd.map(data -> data.toUpperCase());
			JavaRDD<Integer> stringLengthRdd = textRdd.map(data -> data.length());

			stringUpperCaseRdd.collect().forEach(ele -> System.out.print(ele + " "));
			
			System.out.println();

			stringLengthRdd.collect().forEach(ele -> System.out.print(ele + " "));

		}

	}

}
