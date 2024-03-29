package com.elasticsearch.writer;

import java.time.LocalDateTime;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;

/**
 * @author Aqib_Javed
 * @version 1.0
 * @since 09/09/2019
 */
public class StreamWriter {

	private static final String MASTER_NAME = "local[*]";
	private static final String APP_NAME = "StreamWriter";
	private static final String ES_HOST = "127.0.0.1";
	private static final String ES_PORT = "9200";
	private static final String LOG_FILE_PATH = "C:\\ELK\\logevent";
	private static final String CHECK_POINT_LOCATION = "C:\\ELK\\Checkpoints";
	private static final String CHECK_POINT_LOCATION_OPTION = "checkpointLocation";
	private static final String MAX_FILES_PER_TRIGGER_OPTION = "maxFilesPerTrigger";
	private static final String ES_ELASTIC_SEARCH = "org.elasticsearch.spark.sql";
	private static final String ES_RESOURCE_INDEX = "es.resource";
	private static final String ES_NODES = "es.nodes";
	private static final String HEADER_OPTION = "header";
	private static final String HEADER_OPTION_VALUE = "true";
	private static final String OUT_PUT_APPEND = "append";
	private SparkSession sparkSession = null;
	private StructType userSchema = null;
	private Dataset<Row> readDataSets = null;

	/**
	 * @return
	 *         <p>
	 *         this method is created to initialize the data, like spark session,
	 *         and user schema which is build to structured our data and so that we
	 *         can apply filter on KIBANA. Builder pattern is used to called method
	 *         as method chaining.
	 *         </p>
	 */
	public StreamWriter init() {
		sparkSession = SparkSession.builder().config(ConfigurationOptions.ES_NODES, ES_HOST)
				.config(ConfigurationOptions.ES_PORT, ES_PORT).master(MASTER_NAME).appName(APP_NAME).getOrCreate();

		userSchema = new StructType().add("Id", "string").add("Hotel-name", "string").add("Room_avl", "string")
				.add("Is_booked", "string").add("City", "string").add("booking_time", "integer");
		return this;
	}

	/**
	 * @return
	 *         <p>
	 *         Getting datasets as spark structured stream for read, data is read
	 *         from CSV file. and also a timestamp column added for current date
	 *         time.
	 *         </p>
	 */
	public StreamWriter readFromLogFile() {
		readDataSets = sparkSession.readStream().option(HEADER_OPTION, HEADER_OPTION_VALUE)
				.option(MAX_FILES_PER_TRIGGER_OPTION, 1).schema(userSchema).csv(LOG_FILE_PATH)
				.withColumn("timestamp", functions.lit(LocalDateTime.now().toString()));
		return this;
	}

	/**
	 * @return
	 * @throws StreamingQueryException
	 *                                 <p>
	 *                                 writing data to Elastic search, setting check
	 *                                 point location, setting es.resource as index
	 *                                 name, es.nodes with host and port number
	 *                                 </p>
	 */
	public StreamWriter writeToElasticSearch() throws StreamingQueryException {
		StreamingQuery sq = readDataSets.writeStream().outputMode(OUT_PUT_APPEND).format(ES_ELASTIC_SEARCH)
				.option(CHECK_POINT_LOCATION_OPTION, CHECK_POINT_LOCATION).option(ES_RESOURCE_INDEX, getIndexName())
				.option(ES_NODES, ES_HOST + ":" + ES_PORT).start();
		sq.awaitTermination();
		return this;
	}

	private String getIndexName() {
		return "sp_index_test_events";
	}
}
