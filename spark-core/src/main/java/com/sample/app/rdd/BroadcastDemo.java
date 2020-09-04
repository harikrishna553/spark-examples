package com.sample.app.rdd;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastDemo {

	private static final String FILE_NAME = "/empInfo.txt";
	private static final String FILE_NAME_COUNTRY_CODES = "/countryCodes.txt";
	private static final String APP_NAME = "Accumulator Demo";

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
		Map<String, String> countryCodes = loadCountryCodes();

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {

			String filePath = BroadcastDemo.class.getResource(FILE_NAME).getPath();
			JavaRDD<String> empsInfoRdd = javaSparkContext.textFile(filePath);

			Broadcast<Map<String, String>> countryCodesMap = javaSparkContext.broadcast(countryCodes);

			empsInfoRdd.map(data -> {
				String[] splits = data.split(",");
				String result = splits[1] + " belongs to -> " + countryCodesMap.value().get(splits[3].trim());
				return result;
			}).collect().forEach(System.out::println);

		}

	}

	private static Map<String, String> loadCountryCodes() throws FileNotFoundException {
		String filePath = BroadcastDemo.class.getResource(FILE_NAME_COUNTRY_CODES).getPath();

		Scanner scanner = new Scanner(new File(filePath));

		Map<String, String> countryCodes = new HashMap<>();
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();

			String[] splits = line.split(",");
			countryCodes.put(splits[0], splits[1]);

		}

		return countryCodes;
	}

}