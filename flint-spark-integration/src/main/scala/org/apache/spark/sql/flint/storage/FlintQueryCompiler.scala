/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.sql.flint.storage

import scala.io.Source

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.{DateTimeUtils, TimestampFormatter}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.parseColumnPath
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.flint.datatype.FlintDataType.STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS
import org.apache.spark.sql.flint.datatype.FlintMetadataExtensions.MetadataExtension
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Todo. find the right package.
 */
case class FlintQueryCompiler(schema: StructType) {

  /**
   * Using AND to concat predicates. Todo. If spark spark.sql.ansi.enabled = true, more expression
   * defined in V2ExpressionBuilder could be pushed down.
   */
  def compile(predicates: Array[Predicate]): String = {
    if (predicates.isEmpty) {
      return ""
    }
    compile(predicates.reduce(new And(_, _)))
  }

  /**
   * Compile an expression to a query string. Returns empty string if any part of the expression
   * is unsupported.
   */
  def compile(expr: Expression, quoteString: Boolean = true): String = {
    compileOpt(expr, quoteString).getOrElse("")
  }

  /**
   * Compile Expression to Flint query string.
   *
   * @param expr
   *   Expression.
   * @return
   *   empty if does not support.
   */
  def compileOpt(expr: Expression, quoteString: Boolean = true): Option[String] = {
    expr match {
      case LiteralValue(value, dataType) =>
        Some(quote(extract, quoteString)(value, dataType))
      case p: Predicate => visitPredicate(p)
      case f: FieldReference => Some(f.toString())
      case _ => None
    }
  }

  def extract(value: Any, dataType: DataType): String = dataType match {
    case TimestampType =>
      TimestampFormatter(
        STRICT_DATE_OPTIONAL_TIME_FORMATTER_WITH_NANOS,
        DateTimeUtils
          .getZoneId(SQLConf.get.sessionLocalTimeZone),
        false)
        .format(value.asInstanceOf[Long])
    case _ => Literal(value, dataType).toString()
  }

  def quote(f: ((Any, DataType) => String), quoteString: Boolean = true)(
      value: Any,
      dataType: DataType): String =
    dataType match {
      case DateType | TimestampType | StringType if quoteString =>
        s""""${f(value, dataType)}""""
      case _ => f(value, dataType)
    }

  /**
   * Predicate is defined in SPARK filters.scala. Todo.
   *   1. currently, we map spark contains to OpenSearch match query. Can we leverage more full
   *      text queries for text field. 2. configuration of expensive query.
   */
  def visitPredicate(p: Predicate): Option[String] = p.name() match {
    case "IS_NULL" =>
      compileOpt(p.children()(0)).map { field =>
        s"""{"bool":{"must_not":{"exists":{"field":"$field"}}}}"""
      }
    case "IS_NOT_NULL" =>
      compileOpt(p.children()(0)).map { field =>
        s"""{"exists":{"field":"$field"}}"""
      }
    case "AND" =>
      for {
        left <- compileOpt(p.children()(0))
        right <- compileOpt(p.children()(1))
      } yield s"""{"bool":{"filter":[$left,$right]}}"""
    case "OR" =>
      for {
        left <- compileOpt(p.children()(0))
        right <- compileOpt(p.children()(1))
      } yield s"""{"bool":{"should":[{"bool":{"filter":$left}},{"bool":{"filter":$right}}]}}"""
    case "NOT" =>
      compileOpt(p.children()(0)).map { child =>
        s"""{"bool":{"must_not":$child}}"""
      }
    case "=" =>
      for {
        field <- compileOpt(p.children()(0))
        value <- compileOpt(p.children()(1))
        result <-
          if (isTextField(field)) {
            getKeywordSubfield(field) match {
              case Some(keywordField) =>
                Some(s"""{"term":{"$keywordField":{"value":$value}}}""")
              case None => None // Return None for unsupported text fields
            }
          } else {
            Some(s"""{"term":{"$field":{"value":$value}}}""")
          }
      } yield result
    case ">" =>
      for {
        field <- compileOpt(p.children()(0))
        value <- compileOpt(p.children()(1))
      } yield s"""{"range":{"$field":{"gt":$value}}}"""
    case ">=" =>
      for {
        field <- compileOpt(p.children()(0))
        value <- compileOpt(p.children()(1))
      } yield s"""{"range":{"$field":{"gte":$value}}}"""
    case "<" =>
      for {
        field <- compileOpt(p.children()(0))
        value <- compileOpt(p.children()(1))
      } yield s"""{"range":{"$field":{"lt":$value}}}"""
    case "<=" =>
      for {
        field <- compileOpt(p.children()(0))
        value <- compileOpt(p.children()(1))
      } yield s"""{"range":{"$field":{"lte":$value}}}"""
    case "IN" =>
      for {
        field <- compileOpt(p.children()(0))
        valuesList = p.children().tail.flatMap(expr => compileOpt(expr))
        // Only proceed if we have values
        if valuesList.nonEmpty
      } yield {
        val values = valuesList.mkString("[", ",", "]")
        s"""{"terms":{"$field":$values}}"""
      }
    case "STARTS_WITH" =>
      for {
        field <- compileOpt(p.children()(0))
        value <- compileOpt(p.children()(1))
      } yield s"""{"prefix":{"$field":{"value":$value}}}"""
    case "CONTAINS" =>
      for {
        field <- compileOpt(p.children()(0))
        quoteValue <- compileOpt(p.children()(1))
        unQuoteValue <- compileOpt(p.children()(1), false)
      } yield {
        if (isTextField(field)) {
          s"""{"match":{"$field":{"query":$quoteValue}}}"""
        } else {
          s"""{"wildcard":{"$field":{"value":"*$unQuoteValue*"}}}"""
        }
      }
    case "ENDS_WITH" =>
      for {
        field <- compileOpt(p.children()(0))
        value <- compileOpt(p.children()(1), false)
      } yield s"""{"wildcard":{"$field":{"value":"*$value"}}}"""
    case "BLOOM_FILTER_MIGHT_CONTAIN" =>
      for {
        field <- compileOpt(p.children()(0))
        value <- compileOpt(p.children()(1))
      } yield {
        val code = Source.fromResource("bloom_filter_query.script").getLines().mkString(" ")
        s"""
           |{
           |  "bool": {
           |    "filter": {
           |      "script": {
           |        "script": {
           |          "lang": "painless",
           |          "source": "$code",
           |          "params": {
           |            "fieldName": "$field",
           |            "value": $value
           |          }
           |        }
           |      }
           |    }
           |  }
           |}
           |""".stripMargin
      }
    case _ => None
  }

  /**
   * return true if the field is Flint Text field.
   */
  protected def isTextField(attribute: String): Boolean = {
    schema.findNestedField(parseColumnPath(attribute)) match {
      case Some((_, field)) =>
        field.dataType match {
          case StringType =>
            field.metadata.isTextField
          case _ => false
        }
      case None => false
    }
  }

  /**
   * Get keyword subfield name if available for text fields
   */
  protected def getKeywordSubfield(attribute: String): Option[String] = {
    schema.apply(attribute) match {
      case StructField(_, StringType, _, metadata) => metadata.keywordSubfield
      case _ => None
    }
  }
}
