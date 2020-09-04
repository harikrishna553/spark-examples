package com.sample.app.pair.rdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.sample.app.model.Employee;

import scala.Tuple2;

public class MapValuePairRddDemo {
	private static final String APP_NAME = "MapPairRddDemo";

	private static class EmpPairFuncton implements PairFunction<String, Integer, Employee> {

		private static final long serialVersionUID = 987531291L;

		@Override
		public Tuple2<Integer, Employee> call(String t) throws Exception {
			String[] s = t.split(",");

			return new Tuple2<>(Integer.valueOf(s[0]), new Employee(Integer.valueOf(s[0]), s[1]));
		}

	}

	public static void main(String args[]) {

		List<String> empsData = Arrays.asList("1, Krishna", "2, Ram", "3, Sailu", "4, Lahari");

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			JavaRDD<String> empsRdd = javaSparkContext.parallelize(empsData);

			JavaPairRDD<Integer, Employee> empsPairRdd = empsRdd.mapToPair(new EmpPairFuncton());

			JavaPairRDD<Integer, Employee> empsNameInUpperCasePairRdd = empsPairRdd.mapValues(emp -> {
				emp.setFirstName(emp.getFirstName().toUpperCase());
				return emp;
			});

			empsNameInUpperCasePairRdd.collect().forEach(tuple -> {
				System.out.println("id : " + tuple._1() + " emp : " + tuple._2);
			});

		}

	}

}