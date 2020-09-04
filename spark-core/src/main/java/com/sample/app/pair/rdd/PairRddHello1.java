package com.sample.app.pair.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class PairRddHello1 {
	private static final String APP_NAME = "PairRddDemo";

	public static void main(String args[]) {
		Tuple2<Integer, String> emp1 = new Tuple2<>(1, "Krishna");

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			List<Tuple2<Integer, String>> list = Arrays.asList(emp1);

			JavaPairRDD<Integer, String> empsRdd = javaSparkContext.parallelizePairs(list);

			empsRdd.collect().forEach(emp -> {
				System.out.println("id : " + emp._1());
				System.out.println("name : " + emp._2());
			});

		}

	}

}
