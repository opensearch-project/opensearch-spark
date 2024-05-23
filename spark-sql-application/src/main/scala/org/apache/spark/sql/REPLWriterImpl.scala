/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

import org.apache.spark.sql.{DataFrame, FlintJobExecutor, REPLWriter}

class REPLWriterImpl extends REPLWriter {
  override def write(dataFrame: DataFrame, destination: String): Unit = {
    dataFrame.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/dbname")
      .option("dbtable", destination)
      .option("user", "username")
      .option("password", "password")
      .save()
  }
}
