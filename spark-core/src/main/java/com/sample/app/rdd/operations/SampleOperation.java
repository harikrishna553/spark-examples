package com.sample.app.rdd.operations;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SampleOperation {
	private static final String APP_NAME = SampleOperation.class.getName();

	private static List<Integer> getNElements(int noOfElements) {
		List<Integer> list = new ArrayList<>();

		for (int i = 0; i < noOfElements; i++) {
			list.add(i);
		}
		return list;
	}

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		List<Integer> numbers = getNElements(100);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[1]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			JavaRDD<Integer> numbersRdd = javaSparkContext.parallelize(numbers);

			JavaRDD<Integer> sampleDataRdd = numbersRdd.sample(true, 0.2);

			sampleDataRdd.collect().forEach(ele -> System.out.println(ele + " "));

		}

	}

}
