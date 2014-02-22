package com.socrata.pg.soql

import com.socrata.soql.typed.OrderBy
import com.socrata.datacoordinator.id.UserColumnId
import com.socrata.soql.types.{SoQLValue, SoQLType}
import com.socrata.datacoordinator.truth.sql.SqlColumnRep

class OrderBySqlizer(orderBy: OrderBy[UserColumnId, SoQLType]) extends Sqlizer[OrderBy[UserColumnId, SoQLType]] {

  import Sqlizer._

  def sql(rep: Map[UserColumnId, SqlColumnRep[SoQLType, SoQLValue]]) = {
    orderBy.expression.sql(rep) + (if (orderBy.ascending) "" else " desc") + (if (orderBy.nullLast) " nulls last" else "")
  }
}
