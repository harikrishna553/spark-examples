package com.sample.app.joins;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class JoinExample {
	private static final String APP_NAME = "joinDemo";

	public static void main(String args[]) {
		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		List<Tuple2<Integer, Integer>> orderDetails = new ArrayList<>();

		orderDetails.add(new Tuple2<>(1, 1));
		orderDetails.add(new Tuple2<>(1, 2));
		orderDetails.add(new Tuple2<>(2, 3));
		orderDetails.add(new Tuple2<>(5, 4));
		orderDetails.add(new Tuple2<>(6, 11));
		orderDetails.add(new Tuple2<>(7, 12));

		List<Tuple2<Integer, String>> customerDetails = new ArrayList<>();
		customerDetails.add(new Tuple2<>(1, "Krishna"));
		customerDetails.add(new Tuple2<>(2, "Ram"));
		customerDetails.add(new Tuple2<>(3, "Gopi"));
		customerDetails.add(new Tuple2<>(4, "Joel"));
		customerDetails.add(new Tuple2<>(5, "Jitendra"));

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			JavaPairRDD<Integer, Integer> orderDetailsPairRdd = javaSparkContext.parallelizePairs(orderDetails);
			JavaPairRDD<Integer, String> customerDetailsPairRdd = javaSparkContext.parallelizePairs(customerDetails);

			JavaPairRDD<Integer, Tuple2<String, Integer>> joinedData1 = customerDetailsPairRdd
					.join(orderDetailsPairRdd);
			printInfo("Inner Join", joinedData1);

			JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> joinedData2 = customerDetailsPairRdd
					.leftOuterJoin(orderDetailsPairRdd);
			printInfo("Left Outer Join", joinedData2);

			JavaPairRDD<Integer, Tuple2<Optional<String>, Integer>> joinedData3 = customerDetailsPairRdd
					.rightOuterJoin(orderDetailsPairRdd);
			printInfo("Right Outer Join", joinedData3);

			JavaPairRDD<Integer, Tuple2<Optional<String>, Optional<Integer>>> joinedData4 = customerDetailsPairRdd
					.fullOuterJoin(orderDetailsPairRdd);
			printInfo("Full Outer Join", joinedData4);

		}

	}

	private static void printInfo(String title, JavaPairRDD joinedData) {
		System.out.println(title);
		System.out.println("*******************************");
		joinedData.collect().forEach(data -> {
			System.out.println(data);
		});

		System.out.println();
	}

}
