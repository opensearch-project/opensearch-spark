/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.SparkSession

object MyApp {
    def main(args: Array[String]): Unit = {
        var spark = SparkSession.builder()
            .master("local[1]")
            .appName("MyApp")
            .getOrCreate();
        
        println("APP Name :" + spark.sparkContext.appName);
        println("Deploy Mode :" + spark.sparkContext.deployMode);
        println("Master :" + spark.sparkContext.master);

        spark.sql("CREATE table foo (id int, name varchar(100))").show()
        println(">>> Table created")
        spark.sql("SELECT * FROM foo").show()
        println(">>> SQL query of table completed")

	spark.sql("source=foo | fields id").show()
	println(">>> PPL query of table completed")
    }
}
