package com.socrata.bq.soql


import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.bq.store.PostgresUniverseCommon
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.types._
import com.socrata.soql.typed.{StringLiteral, OrderBy, CoreExpr}
import scala.util.parsing.input.NoPosition


class SoQLAnalysisSqlizer(ana: SoQLAnalysis[UserColumnId, SoQLType], tableName: String, allColumnReps: Seq[SqlColumnRep[SoQLType, SoQLValue]])
      extends Sqlizer[Tuple3[SoQLAnalysis[UserColumnId, SoQLType], String, Seq[SqlColumnRep[SoQLType, SoQLValue]]]] {

  import Sqlizer._
  import SqlizerContext._

  val underlying = Tuple3(ana, tableName, allColumnReps)

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[String], ctx: Context, escape: Escape) = {
    sql(false, rep, setParams, ctx, escape)
  }

  /**
   * For rowcount w/o group by, just replace the select with count(*).
   * For rowcount with group by, wrap the original group by sql with a select count(*) from ( {original}) t1
   */
  def rowCountSql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]], setParams: Seq[String], ctx: Context, escape: Escape) = {
    sql(true, rep, setParams, ctx, escape)
  }

  private def sql(reqRowCount: Boolean,
                  rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                  setParams: Seq[String],
                  context: Context,
                  escape: Escape) = {

    val ctx = context + (Analysis -> ana)

    // SELECT
    val ctxSelect = ctx + (SoqlPart -> SoqlSelect)
    val (selectPhrase, setParamsSelect) =
      if (reqRowCount && ana.groupBy.isEmpty) (Seq("count(*)"), setParams)
      else select(rep, setParams, ctxSelect, escape)

    // WHERE
    val where = ana.where.map(_.sql(rep, setParamsSelect, ctx + (SoqlPart -> SoqlWhere), escape))
    val setParamsWhere = where.map(_.setParams).getOrElse(setParamsSelect)

    // GROUP BY
    val groupBy = ana.groupBy.map { (groupBys: Seq[CoreExpr[UserColumnId, SoQLType]]) =>
      groupBys.foldLeft(Tuple2(Seq.empty[String], setParamsWhere)) { (t2, gb: CoreExpr[UserColumnId, SoQLType]) =>
      val BQSql(sql, newSetParams) = gb.sql(rep, t2._2, ctx + (SoqlPart -> SoqlGroup), escape)
        val modifiedSQL = sql.replaceAll("[)(*]", "_") // to reference possible aliases in the SELECT stmt
      (t2._1 :+ modifiedSQL, newSetParams)
    }}
    val setParamsGroupBy = groupBy.map(_._2).getOrElse(setParamsWhere)

    // HAVING
    val having = ana.having.map(_.sql(rep, setParamsGroupBy, ctx + (SoqlPart -> SoqlHaving), escape))
    val setParamsHaving = having.map(_.setParams).getOrElse(setParamsGroupBy)

    // ORDER BY
    val orderBy = ana.orderBy.map { (orderBys: Seq[OrderBy[UserColumnId, SoQLType]]) =>
      orderBys.foldLeft(Tuple2(Seq.empty[String], setParamsHaving)) { (t2, ob: OrderBy[UserColumnId, SoQLType]) =>
        val BQSql(sql, newSetParams) =
          ob.sql(rep, t2._2, ctx + (SoqlPart -> SoqlOrder) + (RootExpr -> ob.expression), escape)
        val modifiedSQL = sql.replaceAll("[)(*]", "_") // to reference possible aliases in the SELECT stmt
        (t2._1 :+ modifiedSQL, newSetParams)
      }}

    val setParamsOrderBy = orderBy.map(_._2).getOrElse(setParamsHaving)

    // COMPLETE SQL
    val completeSql = funcAlias(selectPhrase).mkString("SELECT ", ",", "") +
      s" FROM $tableName" +
      where.map(" WHERE " +  _.sql).getOrElse("") +
      groupBy.map(_._1.mkString(" GROUP BY ", ",", "")).getOrElse("") +
      having.map(" HAVING " +  _.sql).getOrElse("") +
      orderBy.map(_._1.mkString(" ORDER BY ", ",", "")).getOrElse("") +
      ana.limit.map(" LIMIT " + _.toString).getOrElse("")

    BQSql(countBySubQuery(reqRowCount, completeSql), setParamsOrderBy)
  }

  private def countBySubQuery(reqRowCount: Boolean, sql: String) = {
    if (reqRowCount && ana.groupBy.isDefined) s"SELECT count(*) FROM ($sql) t1"
    else sql
  }

  private def select(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]],
                     setParams: Seq[String],
                     ctx: Context,
                     escape: Escape) = {
    ana.selection.foldLeft(Tuple2(Seq.empty[String], setParams)) { (t2, columnNameAndcoreExpr) =>
      val (columnName, coreExpr) = columnNameAndcoreExpr
      val BQSql(sql, newSetParams) = coreExpr.sql(rep, t2._2, ctx + (RootExpr -> coreExpr), escape)
      val timeStampConv = if (coreExpr.typ.toString.contains("timestamp")) s"TIMESTAMP_TO_USEC($sql)" else sql
      (t2._1 :+ timeStampConv, newSetParams)
    }
  }

  /**
   * Maps each function in the select statement so that it can be referenced outside of the SELECT clause
   * statement if it is present there.
   */
  private def funcAlias(select: Seq[String]): Seq[String] = {
    select.map(sql =>
      if(sql.matches("\\((sum|avg|count|min|max)\\(.*\\)\\)")) {
        s"$sql AS ${sql.replaceAll("[)(*]", "_")}"
      } else {
        sql
      })
  }
}
