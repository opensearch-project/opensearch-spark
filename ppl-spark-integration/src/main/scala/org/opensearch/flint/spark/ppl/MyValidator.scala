/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import scala.collection.JavaConverters._

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.sql.parser.SqlParserPos.ZERO
import org.apache.calcite.sql.validate.SqlValidator.Config
import org.apache.calcite.sql.validate.SqlValidatorImpl
import org.apache.calcite.sql.{SqlNodeList, SqlOperatorTable, SqlSelect}

class MyValidator(opTab: SqlOperatorTable, catalogReader: CalciteCatalogReader, typeFactory: JavaTypeFactory, config: Config)
  extends SqlValidatorImpl(opTab, catalogReader, typeFactory, config) {

  override def expandStar(selectList: SqlNodeList, select: SqlSelect, includeSystemVars: Boolean): SqlNodeList = {
    val (starExcepts, others) = selectList.asScala.partition(_.isInstanceOf[StarExcept])
    val starExceptList = starExcepts.flatMap(starExcept => {
      val originList = super.expandStar(SqlNodeList.of(starExcept), select, includeSystemVars)
      val exceptList = super.expandStar(starExcept.asInstanceOf[StarExcept].exceptList, select, includeSystemVars)
      val exceptListStr = exceptList.asScala.map(_.toString)
      originList.removeIf(item => exceptListStr.contains(item.toString))
      originList.asScala
    })
    val otherList = super.expandStar(SqlNodeList.of(ZERO, others.asJava), select, includeSystemVars)
    val expandedList = SqlNodeList.of(ZERO, (otherList.asScala ++ starExceptList).asJava)
    getRawSelectScope(select).setExpandedSelectList(expandedList)
    expandedList
  }

}
