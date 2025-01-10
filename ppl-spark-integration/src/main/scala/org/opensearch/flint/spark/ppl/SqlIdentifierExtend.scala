/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl


import com.google.common.collect.ImmutableList
import lombok.Getter
import scala.collection.JavaConverters._

import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.parser.SqlParserPos.ZERO
import org.apache.calcite.sql.{SqlIdentifier, SqlNodeList, SqlWriter}

@Getter
case class StarExcept(exceptList: SqlNodeList)(names: Seq[String] = Seq(""), pos: SqlParserPos = ZERO)
  extends SqlIdentifier(names.asJava, pos) {

  override def toString: String = {
    super.toString + " EXCEPT " + exceptList.toString
  }

  override def unparse(writer: SqlWriter, leftPrec: Int, rightPrec: Int): Unit = {
    super.unparse(writer, leftPrec, rightPrec)
    writer.keyword("EXCEPT")
    writer.list(SqlWriter.FrameTypeEnum.PARENTHESES, SqlWriter.COMMA, exceptList)
  }
}
