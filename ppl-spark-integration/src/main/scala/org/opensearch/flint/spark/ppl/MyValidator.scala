/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.flint.spark.ppl

import scala.collection.JavaConverters._

import org.apache.calcite.adapter.java.JavaTypeFactory
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataType
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
      originList.asScala.filter(item => !exceptListStr.contains(item.toString))
    })
    val otherList = super.expandStar(SqlNodeList.of(ZERO, others.asJava), select, includeSystemVars)
    val expandedList = SqlNodeList.of(ZERO, (otherList.asScala ++ starExceptList).asJava)
    getRawSelectScope(select).setExpandedSelectList(expandedList)
    expandedList
  }

  override def validateSelectList (selectItems: SqlNodeList, select: SqlSelect, targetRowType: RelDataType): RelDataType = {
    val (starExcepts, others) = selectItems.asScala.partition(_.isInstanceOf[StarExcept])
    val (starExceptList, starExceptExpandedList) = starExcepts.map(starExcept => {
      val originList = super.validateSelectList(SqlNodeList.of(starExcept), select, targetRowType)
      val originExpandedList = getRawSelectScope(select).getExpandedSelectList
      val exceptList = super.validateSelectList(starExcept.asInstanceOf[StarExcept].exceptList, select, targetRowType)
      val exceptListStr = exceptList.getFieldNames.asScala
      val exceptExpandedList = getRawSelectScope(select).getExpandedSelectList
      val exceptExpandedListStr = exceptExpandedList.asScala.map(_.toString)
      (originList.getFieldList.asScala.filter(field => !exceptListStr.contains(field.getName)),
        originExpandedList.asScala.filter(item => !exceptExpandedListStr.contains(item.toString)))
    }).unzip
    val otherList = super.validateSelectList(SqlNodeList.of(ZERO, others.asJava), select, targetRowType)
    val newSelectItems = (getRawSelectScope(select).getExpandedSelectList.asScala ++ starExceptExpandedList.flatten).asJava
    if (config.identifierExpansion) {
      select.setSelectList(SqlNodeList.of(ZERO, newSelectItems))
    }
    getRawSelectScope(select).setExpandedSelectList(newSelectItems)
    typeFactory.createStructType((otherList.getFieldList.asScala ++ starExceptList.flatten).asJava)
  }

}
