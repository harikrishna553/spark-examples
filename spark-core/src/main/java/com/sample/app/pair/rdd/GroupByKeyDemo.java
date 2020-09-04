package com.sample.app.pair.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.sample.app.demo.WordCountDemo;

import scala.Tuple2;

public class GroupByKeyDemo {

	private static final String FILE_NAME = "/logFile.txt";
	private static final String APP_NAME = "GroupByKeyDemo";

	private static class LogMsgPairFunction implements PairFunction<String, Integer, String> {

		private static final long serialVersionUID = 987531291L;

		@Override
		public Tuple2<Integer, String> call(String logMsg) throws Exception {

			int firstSpaceIndex = logMsg.indexOf(" ");

			return new Tuple2<Integer, String>(Integer.valueOf(logMsg.substring(0, firstSpaceIndex).trim()),
					logMsg.substring(firstSpaceIndex, logMsg.length()));
		}

	}

	public static void main(String[] args) throws Exception {

		Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);

		SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster("local[3]");

		try (JavaSparkContext javaSparkContext = new JavaSparkContext(conf)) {
			String filePath = WordCountDemo.class.getResource(FILE_NAME).getPath();

			JavaRDD<String> logMessages = javaSparkContext.textFile(filePath);

			JavaPairRDD<Integer, String> userLogMsgsById = logMessages.mapToPair(new LogMsgPairFunction());

			JavaPairRDD<Integer, Iterable<String>> logMessagesById = userLogMsgsById.groupByKey();

			logMessagesById.collectAsMap().entrySet().forEach(entry -> {
				System.out.println(entry.getKey() + " : " + entry.getValue());
			});

		}

	}

}
