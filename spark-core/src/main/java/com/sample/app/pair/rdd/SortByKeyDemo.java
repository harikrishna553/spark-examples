package com.sample.app.pair.rdd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SortByKeyDemo {
	private static final String APP_NAME = "SortByKey";

	private static Map<Integer, Double> empAvgSalaryByExperience() {
		Map<Integer, Double> map = new HashMap<>();
		map.put(1, 380000d);
		map.put(3, 600000d);
		map.put(2, 410000d);
		map.put(5, 800000d);
		map.put(7, 110000d);
		map.put(6, 950000d);
		map.put(4, 650000d);

		return map;
	}

	private static List<Tuple2<Integer, Double>> getList(Map<Integer, Double> map) {

		List<Tuple2<Integer, Double>> list = new ArrayList<>();

		for (Integer key : map.keySet()) {
			list.add(new Tuple2<Integer, Double>(key, map.get(key)));
		}

		return list;

	}

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			Map<Integer, Double> empSalaryByExp = empAvgSalaryByExperience();
			List<Tuple2<Integer, Double>> tuples = getList(empSalaryByExp);

			JavaPairRDD<Integer, Double> pairRdd = javaSparkContext.parallelizePairs(tuples);

			pairRdd.sortByKey(false).collect().forEach(obj -> {
				System.out.println(obj._1() + " -> " + obj._2);

			});

		}

	}

}
