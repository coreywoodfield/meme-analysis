package edu.byu.cs.dml.redditcleaner;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.text.Normalizer;

import static org.apache.spark.sql.functions.*;

public class CleanComments {

	public static void main(String[] args) {
		SparkSession spark = new SparkSession.Builder().appName("sorter").getOrCreate();
//		spark.sqlContext().udf().register("clean_body", (UDF1<String, String>) s ->
//				Normalizer.normalize(s, Normalizer.Form.NFD)
//						  .replaceAll("(?s)\\*\\*(.+?)\\*\\*", "$1")
//						  // remove italics
//						  .replaceAll("(?s)\\*(.+?)\\*", "$1")
//						  // strikethrough
//						  .replaceAll("(?s)~~(.+?)~~", "$1")
//						  // code
//						  .replaceAll("(?s)`(.*?)`", "")
//						  .replaceAll("(?m)^ {4}.*$", "")
//						  .replaceAll("(?m)^\t.*$", "")
//						  // links with title text
//						  .replaceAll("(?s)\\[(.*?)]\\(\\S*+\\s(.*?)(?<!\\\\)\\)", "$1 ($2)")
//						  // links without title text
//						  .replaceAll("(?s)\\[(.*?)]\\(.*?(?<!\\\\)\\)", "$1")
//						  // unformatted links
//						  .replaceAll("(?x)" +
//									  "(?:https?://)?+										# protocol\n" +
//									  "[a-zA-Z0-9](?:[-a-zA-Z0-9]+[a-zA-Z0-9])?				# first section of domain\n" +
//									  "(?:\\.[a-zA-Z0-9](?:[-a-zA-Z0-9]+[a-zA-Z0-9])?)++	# more sections of domain, separated by '.'\n" +
//									  "(?:/\\S*)?											# the path, and any query parameters and such",
//								  "")
//						  // superscript
//						  .replaceAll("(?<!\\s|\\\\)\\^", " ")
//						  // misc. formatting punctuation
//						  .replaceAll("(?m)^\\*\\*\\*$", "")
//						  .replaceAll("(?m)^#{1,6}", "")
//						  // quotes
//						  .replaceAll("(?m)^&gt;", "")
//						  // subreddits/usernames
//						  .replaceAll("/r/([A-Za-z0-9]*)", "$1")
//						  .replaceAll("/u/[A-Za-z0-9]*", "")
//						  .replaceAll("\\s+", " ")
//						  // non-ascii
//						  .replaceAll("[^ -~]", ""), DataTypes.StringType);
		spark.read()
			 .option("header", true)
			 .csv(args[0])
			 .where(col("body").isNotNull()
							   .and(col("body").notEqual(""))
							   .and(not(col("author").rlike("(?:[^A-Z]Bot|\bbot)[^a-z]"))))
			 .select(col("author"), /*callUDF("clean_body",*/ col("body")/*).as("body")*/, col("subreddit"), col("created_utc"))
//			 .where(col("body").notEqual(""))
			 .sort("author")
			 .write()
			 .option("header", true)
			 .option("compression", "gzip")
			 .csv(args[1]);
	}
}
