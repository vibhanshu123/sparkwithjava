package com.jobreadyprogrammer.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class InferCSVSchema {
	
	public void printSchema() {
		SparkSession spark = SparkSession.builder()
		        .appName("Complex CSV to Dataframe")
		        .master("local")
		        .getOrCreate();
		 
		    Dataset<Row> df = spark.read().format("csv") //
		        .option("header", "true") // TO SAY THAT THE FILE HAS A HEADER
		        .option("multiline", true) //  TO SAY THAT INPUT FILE HAS MULTILINE 
		        .option("sep", ";") // TO SAY THAT THE SEPERATOR IS SEMICOLON
		        .option("quote", "^") // it means ^ will be interpreted as quotes , i am telling spark to treat that character as quote
		        .option("dateFormat", "M/d/y") // TO UNDERSTAND THAT DATE FORMAT IS M/D/Y
		        .option("inferSchema", true) // no hardcoding of schema and leaving spark to infer schema
		        .load("src/main/resources/amazonProducts.txt");
		 
		    System.out.println("Excerpt of the dataframe content:");
//		    df.show(7);
		    df.show(7, 10); // truncate after 10 chars
		    System.out.println("Dataframe's schema:");
		    df.printSchema();
	}
	
}
