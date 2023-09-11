package org.opensearch.flint.spark

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable

class FlintSparkParserChain (sparkParser: ParserInterface, parserChain: Seq[ParserInterface]) extends ParserInterface {

  private val parsers: mutable.ListBuffer[ParserInterface] = mutable.ListBuffer() ++= parserChain

  /**
   * this method goes threw the parsers chain and try parsing sqlText - if successfully return the logical plan
   * otherwise go to the next parser in the chain and try to parse the sqlText
   *
   * @param sqlText
   * @return
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    try {
      // go threw the parsers chain and try parsing sqlText - if successfully return the logical plan
      // otherwise go to the next parser in the chain and try to parse the sqlText
      for (parser <- parsers) {
        try {
          return parser.parsePlan(sqlText)
        } catch {
          case _: Exception => // Continue to the next parser
        }
      }
      // Fall back to Spark parse plan logic if all parsers in the chain fail
      sparkParser.parsePlan(sqlText)
    }
  }


  override def parseExpression(sqlText: String): Expression = sparkParser.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    sparkParser.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    sparkParser.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    sparkParser.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    sparkParser.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType = sparkParser.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan = sparkParser.parseQuery(sqlText)
}
