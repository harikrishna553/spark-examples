package com.sample.app.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import scala.Option;

public class AccumulatorDemo {

	private static final String FILE_NAME = "/empInfo.txt";
	private static final String APP_NAME = "Accumulator Demo";

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");
		SparkContext sparkContext = new SparkContext(conf);

		final LongAccumulator empsMissingLastNames = new LongAccumulator();
		final LongAccumulator empsMissingLastNamesInIndia = new LongAccumulator();
		empsMissingLastNames.register(sparkContext, Option.apply("empsMissingLastNames"), false);
		empsMissingLastNamesInIndia.register(sparkContext, Option.apply("empsMissingLastNamesInIndia"), false);

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext)) {

			String filePath = AccumulatorDemo.class.getResource(FILE_NAME).getPath();

			JavaRDD<String> empsInfoRdd = javaSparkContext.textFile(filePath);

			JavaRDD<String> empsFromIndia = empsInfoRdd.filter(resp -> {
				String[] splits = resp.split(",");
				if (splits[2].isEmpty()) {
					empsMissingLastNames.add(1l);
				}

				if (splits[2].isEmpty() && splits[3].equals("India")) {
					empsMissingLastNamesInIndia.add(1l);
				}

				return splits[3].equals("India");
			});

			System.out.println("Employees from India");
			empsFromIndia.collect().forEach(System.out::println);

			System.out.println("Total employees missing last names : " + empsMissingLastNames);
			System.out.println("Total employees missing last names in india : " + empsMissingLastNamesInIndia);

		}

	}

}