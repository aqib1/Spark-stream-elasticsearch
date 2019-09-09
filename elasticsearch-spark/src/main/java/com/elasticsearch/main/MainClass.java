package com.elasticsearch.main;

import org.apache.spark.sql.streaming.StreamingQueryException;

import com.elasticsearch.writer.StreamWriter;

public class MainClass {
	public static void main(String[] args) {
		try {
			new StreamWriter().init().readFromLogFile().writeToElasticSearch();
		} catch (StreamingQueryException e) {
			e.printStackTrace();
		}
	}
}
