package com.socrata.bq.soql

import com.socrata.datacoordinator.id.{ColumnId, UserColumnId}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep
import com.socrata.soql.SoQLAnalysis
import com.socrata.soql.typed._
import com.socrata.soql.types._
import com.socrata.bq.soql.SqlizerContext.SqlizerContext


case class BQSql(sql: String, setParams: Seq[String]) {

  // This is disgusting
  def injectParams: String = {
    var i = -1
    val result = new StringBuilder()
    sql.split(" ").foreach {
      case "?" =>
        i += 1
        result.append(setParams(i) + " ")
      case str => result.append(str + " ")
    }
    result.toString()
  }
}

trait Sqlizer[T] {

  import Sqlizer._
  import SqlizerContext._

  def sql(physicalColumnMapping: Map[UserColumnId, String], setParams: Seq[String], ctx: Context, escape: Escape): BQSql

  val underlying: T

  protected def useUpper(ctx: Context): Boolean = {
    if (caseInsensitive(ctx))
      ctx(SoqlPart) match {
        case SoqlWhere | SoqlGroup | SoqlHaving => true
        case SoqlSelect => usedInGroupBy(ctx)
        case SoqlSearch => false
        case _ => false
      }
    else false
  }

  protected def appendWildCard(ctx: Context) : Boolean = {
    false
  }

  protected def usedInGroupBy(ctx: Context): Boolean = {
    val rootExpr = ctx.get(RootExpr)
    ctx(SoqlPart) match {
      case SoqlSelect | SoqlOrder =>
        ctx.get(Analysis) match {
          case Some(analysis: SoQLAnalysis[_, _]) =>
            analysis.groupBy match {
              case Some(groupBy) =>
                // Use upper in select if this expression or the selected expression it belongs to is found in group by
                groupBy.exists(expr => (underlying == expr) || rootExpr.exists(_ == expr))
              case None => false
            }
          case _ => false
        }
      case SoqlSearch => false
      case _ => false
    }
  }

  protected val ParamPlaceHolder: String = "?"

  private def caseInsensitive(ctx: Context): Boolean =
    ctx.contains(CaseSensitivity) && ctx(CaseSensitivity) == CaseInsensitive
}

object Sqlizer {

  type Context = Map[SqlizerContext, Any]


  implicit def stringLiteralSqlizer(lit: StringLiteral[SoQLType]): Sqlizer[StringLiteral[SoQLType]] = {
    new StringLiteralSqlizer(lit)
  }

  implicit def functionCallSqlizer(lit: FunctionCall[UserColumnId, SoQLType]): Sqlizer[FunctionCall[UserColumnId, SoQLType]] = {
    new FunctionCallSqlizer(lit)
  }

  implicit def coreExprSqlizer(expr: CoreExpr[UserColumnId, SoQLType]): Sqlizer[_] = {
    expr match {
      case fc: FunctionCall[UserColumnId, SoQLType] => new FunctionCallSqlizer(fc)
      case cr: ColumnRef[UserColumnId, SoQLType] => new ColumnRefSqlizer(cr)
      case lit: StringLiteral[SoQLType] => new StringLiteralSqlizer(lit)
      case lit: NumberLiteral[SoQLType] => new NumberLiteralSqlizer(lit)
      case lit: BooleanLiteral[SoQLType] => new BooleanLiteralSqlizer(lit)
      case lit: NullLiteral[SoQLType] => new NullLiteralSqlizer(lit)
    }
  }

  implicit def orderBySqlizer(ob: OrderBy[UserColumnId, SoQLType]): Sqlizer[OrderBy[UserColumnId, SoQLType]] = {
    new OrderBySqlizer(ob)
  }

  implicit def analysisSqlizer(analysisTable: Tuple2[SoQLAnalysis[UserColumnId, SoQLType], String]) = {
    new SoQLAnalysisSqlizer(analysisTable._1, analysisTable._2)
  }


}

object SqlizerContext extends Enumeration {
  type SqlizerContext = Value
  val Analysis = Value("analysis")
  val SoqlPart = Value("soql-part")
  val SoqlSelect = Value("select")
  val SoqlWhere = Value("where")
  val SoqlGroup = Value("group")
  val SoqlHaving = Value("having")
  val SoqlOrder = Value("order")
  val SoqlSearch = Value("search")
  val Extras = Value("extras")
  // Need to append % after the string
  val BeginsWith = Value("begins-with")
  val IdRep = Value("id-rep")
  val VerRep = Value("ver-rep")
  val RootExpr = Value("root-expr")
  val CaseSensitivity = Value("case-sensitivity")
}


sealed trait CaseSensitivity

object CaseInsensitive extends CaseSensitivity

object CaseSensitive extends CaseSensitivity
