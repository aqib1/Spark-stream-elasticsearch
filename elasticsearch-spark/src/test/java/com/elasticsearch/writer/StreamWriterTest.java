package com.elasticsearch.writer;

import java.time.LocalDateTime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StreamWriterTest {

	private SparkSession sparkSession = null;
	private static final String MASTER_NAME = "local[*]";
	private static final String APP_NAME = "StreamWriter";
	private static final String ES_HOST = "127.0.0.1";
	private static final String ES_PORT = "9200";
	private static final String LOG_FILE_PATH = "C:\\ELK\\logevent";
	private static final String HEADER_OPTION = "header";
	private static final String HEADER_OPTION_VALUE = "true";
	private static final String MAX_FILES_PER_TRIGGER_OPTION = "maxFilesPerTrigger";

	@Before
	public void initSparkSession() {
		sparkSession = SparkSession.builder().config(ConfigurationOptions.ES_NODES, ES_HOST)
				.config(ConfigurationOptions.ES_PORT, ES_PORT).master(MASTER_NAME).appName(APP_NAME).getOrCreate();
	}

	@Test
	public void testDataInCSV() {
		StructType userSchema = new StructType().add("Id", "string").add("Hotel-name", "string")
				.add("Room_avl", "string").add("Is_booked", "string").add("City", "string")
				.add("booking_time", "integer");
		Dataset<Row> readDataSets = sparkSession.readStream().option(HEADER_OPTION, HEADER_OPTION_VALUE)
				.option(MAX_FILES_PER_TRIGGER_OPTION, 1).schema(userSchema).csv(LOG_FILE_PATH)
				.withColumn("timestamp", functions.lit(LocalDateTime.now().toString()));
		Assert.assertNotNull(readDataSets);
	}

	@After
	public void closeSparkSession() {
		sparkSession.stop();
	}

}
