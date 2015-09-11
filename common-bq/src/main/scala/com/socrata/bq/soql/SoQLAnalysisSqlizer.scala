package com.socrata.bq.soql

import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types._
import com.socrata.soql.typed.{OrderBy, CoreExpr}

/**
 * Converts SoQLAnalysis and a table name into a complete BigQuery SQL.
 * @param analysis The analysis containing information about what to store in each of the clauses of the SQL statement.
 * @param tableName The BigQuery table name that the statement should query.
 */
class SoQLAnalysisSqlizer(analysis: SoQLAnalysis[UserColumnId, SoQLType], tableName: String)
      extends Sqlizer[Tuple2[SoQLAnalysis[UserColumnId, SoQLType], String]] {

  import Sqlizer._
  import SqlizerContext._

  val underlying = Tuple2(analysis, tableName)
  val convToUnderScore = "[)./+\\-?(*<> ]"  // Set of chars that need to be replaced by an underscore
  var aliasMap = scala.collection.mutable.Map[String, String]() // Mapping from expressions that need to be aliased to their aliases

  /**
   * Combines information from the analysis, column mapping from user column ids to physical columns, and the context
   * to construct a coherent SQL String without parameters and a sequence of parameters to be inserted into the SQL string.
   * @param physicalColumnMapping The mapping from each UserColumnId to a physical column present in the BQ table.
   * @param setParams An empty sequence.
   * @param context A map that contains meta-data about how the SQL string is to be constructed.
   * @param escape A function that escapes illegal characters as specified by BigQuery.
   * @return A BQSql(sql, params) where sql is the incomplete SQL string with "?" as placeholders
   *         for each element in params.
   */
  override def sql(physicalColumnMapping: Map[UserColumnId, String], setParams: Seq[String], context: Context, escape: Escape) = {
    val ctx = context + (Analysis -> analysis)

    // The way that this works:
    // Sqlize each column of each phrase, SELECT, WHERE, GROUP BY, HAVING, and ORDER BY
    // to convert the user column ids to physical columns and apply appropriate function calls around them (sum, min, max).
    // Keep on building upon this SQL String until all phrases are complete.
    //
    // Rather than embedding the parameters directly in the SQL string, to avoid SQL injection, a Seq[String], setParams,
    // is built up in each phrase and passed onto each subsequent phrase to be escaped and manually inserted into the
    // SQL string by the caller.

    // SELECT
    val ctxSelect = ctx + (SoqlPart -> SoqlSelect)
    val (selectPhrase, setParamsSelect) = select(physicalColumnMapping, setParams, ctxSelect, escape)

    // WHERE
    val where = analysis.where.map(_.sql(physicalColumnMapping, setParamsSelect, ctx + (SoqlPart -> SoqlWhere), escape))
    val setParamsWhere = where.map(_.setParams).getOrElse(setParamsSelect)

    // GROUP BY
    val groupBy = analysis.groupBy.map { (groupBys: Seq[CoreExpr[UserColumnId, SoQLType]]) =>
      groupBys.foldLeft(Tuple2(Seq.empty[String], setParamsWhere)) { (t2, gb: CoreExpr[UserColumnId, SoQLType]) =>
      val BQSql(sql, newSetParams) = gb.sql(physicalColumnMapping, t2._2, ctx + (SoqlPart -> SoqlGroup), escape)
      val soqlType = gb.typ
      // to reference possible aliases in the SELECT stmt
      val modifiedSQL = soqlType match {
        // the wrapper TIMESTAMP_TO_MSEC may have been introduced before the alias was added to the map. to reference it
        // correctly, TIMESTAMP_TO_MSEC is wrapped around the sql in an attempt to get the alias, otherwise, grab it regularly
        // from the aliasMap.
        case SoQLFixedTimestamp | SoQLFloatingTimestamp => aliasMap.getOrElse(s"TIMESTAMP_TO_MSEC($sql)", aliasMap.getOrElse(sql, sql))
        case _ => aliasMap.getOrElse(sql, sql)
      }
      (t2._1 :+ modifiedSQL, newSetParams)
    }}
    val setParamsGroupBy = groupBy.map(_._2).getOrElse(setParamsWhere)

    // HAVING
    val having = analysis.having.map(_.sql(physicalColumnMapping, setParamsGroupBy, ctx + (SoqlPart -> SoqlHaving), escape))
    val setParamsHaving = having.map(_.setParams).getOrElse(setParamsGroupBy)

    // ORDER BY
    val orderBy = analysis.orderBy.map { (orderBys: Seq[OrderBy[UserColumnId, SoQLType]]) =>
      orderBys.foldLeft(Tuple2(Seq.empty[String], setParamsHaving)) { (t2, ob: OrderBy[UserColumnId, SoQLType]) =>
        val BQSql(sql, newSetParams) =
          ob.sql(physicalColumnMapping, t2._2, ctx + (SoqlPart -> SoqlOrder) + (RootExpr -> ob.expression), escape)
        val modifiedSQL = sql.contains("desc") match {
          case true => s"${aliasMap.getOrElse(sql.substring(0, sql.indexOf("desc")).trim, sql)}"
          case false => aliasMap.getOrElse(sql, sql)
        }
        (t2._1 :+ modifiedSQL, newSetParams)
      }}

    val setParamsOrderBy = orderBy.map(_._2).getOrElse(setParamsHaving)

    // COMPLETE SQL
    val completeSql = selectPhrase.mkString("SELECT ", ",", "") +
      s" FROM $tableName" +
      where.map(" WHERE " +  _.sql).getOrElse("") +
      groupBy.map(_._1.mkString(" GROUP BY ", ",", "")).getOrElse("") +
      having.map(" HAVING " +  _.sql).getOrElse("") +
      orderBy.map(_._1.mkString(" ORDER BY ", ",", "")).getOrElse("") +
      analysis.limit.map(" LIMIT " + _.toString).getOrElse("") +
      analysis.offset.map(" OFFSET " + _.toString).getOrElse("")

    BQSql(completeSql, setParamsOrderBy)
  }

  /**
   * Constructs the full select statement.
   *
   * @param physicalColumnMapping The mapping from each UserColumnId to a physical column present in the BQ table.
   * @param setParams The current sequence of parameters to be injected into the SQL string.
   * @param ctx A map that contains meta-data about how the SQL string is to be constructed.
   * @param escape A function that escapes illegal characters as specified by BigQuery.
   * @return A BQSql(sql, params) where sql is the complete aliased select statement and params represents the params
   *         to be inserted into the select statement.
   */
  private def select(physicalColumnMapping: Map[UserColumnId, String],
                     setParams: Seq[String],
                     ctx: Context,
                     escape: Escape) = {
    val (conv, params) = analysis.selection.foldLeft(Tuple2(Seq.empty[String], setParams)) { (t2, columnNameAndcoreExpr) =>
      val (columnName, coreExpr) = columnNameAndcoreExpr
      val BQSql(sql, newSetParams) = coreExpr.sql(physicalColumnMapping, t2._2, ctx + (RootExpr -> coreExpr), escape)
      val soqlType = coreExpr.typ

      val conversion = soqlType match {
        case SoQLFixedTimestamp   // need to wrap timestamp around
           | SoQLFloatingTimestamp => if (!sql.matches(".*(day|year|month).*")) s"TIMESTAMP_TO_MSEC($sql)" else sql
        case SoQLPoint => s"$sql.lat, $sql.long"  // column represents a nested field in BQ, lat, long
        case _ => sql
      }
      (t2._1 :+ conversion, newSetParams)
    }
    (funcAlias(conv), params)
  }

  /**
   * Maps each function in the select statement to an alias so that it can be referenced outside of the SELECT clause
   * statement if it is present there.
   *
   * Example:
   *
   * ['sum(u_ty3g_fj3t)', 'u_t3ty_f2g3'] => ['sum(u)ty3g_fj3t) AS sum_u_ty3g_fj3t__0]', 'u_t3ty_f2g3']
   *
   * @param select A sequence of strings representing columns to be selected.
   * @return A modified version of the column names with function name calls and symbols aliased with the AS keyword to
   *         the same string replaced with _.
   */
  private def funcAlias(select: Seq[String]): Seq[String] = {
    select.zipWithIndex.map{
      case (sql, index) =>
        // check if it matches one of the aggregates and it doesn't contain any spaces.
      if (sql.toLowerCase.matches(".*\\w*\\(.*\\).*")) {
        val alias = s"__$index"
        val newSql = s"$sql AS $alias"
        aliasMap += (sql -> alias)
        newSql
      } else {
        sql
      }
    }
  }

}
