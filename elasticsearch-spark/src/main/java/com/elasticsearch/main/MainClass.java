package com.elasticsearch.main;

import org.apache.spark.sql.streaming.StreamingQueryException;

import com.elasticsearch.writer.StreamWriter;

/**
 * @author Aqib_Javed
 * @version 1.0
 * @since 09/09/2019
 *
 */
public class MainClass {
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			new StreamWriter().init().readFromLogFile().writeToElasticSearch();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}
	}
}
