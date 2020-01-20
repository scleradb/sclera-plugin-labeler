/**
* Sclera - Row Labeler
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.plugin.analytics.sequence.labeler

import scala.language.postfixOps

import com.scleradb.sql.expr._
import com.scleradb.sql.result.ScalTableRow

import com.scleradb.util.automata.datatypes.Label
import com.scleradb.util.tools.Counter

import com.scleradb.analytics.sequence.labeler.PredRowLabeler

// labels rows based on predicates
class MatPredLabeler(
    override val predLabels: List[(ScalExpr, Label)]
) extends PredRowLabeler {
    override def requiredCols: List[ColRef] =
        predLabels.flatMap { case (expr, _) => expr.colRefs } distinct

    // alias needed to distinguish a label from
    // an input columns with the same name
    private val labelAlias: List[(Label, ColRef)] = predLabels.map {
        case (_, label) => (label -> ColRef(Counter.nextSymbol(label.id)))
    }

    private val labelAliasMap: Map[Label, ColRef] = Map() ++ labelAlias

    override def rowLabels(row: ScalTableRow): List[Label] =
        labelAlias.toList.flatMap {
            case (label, ColRef(cname)) =>
                val isPresent: Boolean = row.getBooleanOpt(cname) getOrElse {
                    throw new RuntimeException {
                        "Found unexpected NULL in column \"" + cname + "\"" +
                        " for label \"" + label + "\""
                    }
                }

                if( isPresent ) Option(label) else None
        }

    override def labeledExpr(input: RelExpr): RelExpr = {
        val labelTargets: List[AliasedExpr] = predLabels.map {
            case (pred, label) => AliasedExpr(pred, labelAliasMap(label))
        }

        val inpTargets: List[AliasedExpr] =
            input.tableColRefs.map { cRef => AliasedExpr(cRef, cRef) }
        val targets: List[AliasedExpr] = labelTargets:::inpTargets

        // respect the order, if any
        input match {
            case RelOpExpr(order: Order, List(bExpr), locIdOpt) =>
                // order needs to be placed on the top, after the project
                RelOpExpr(order, List(RelOpExpr(Project(targets), List(bExpr))),
                          locIdOpt)

            case _ =>
                val labeledExpr: RelExpr =
                    RelOpExpr(Project(targets), List(input))

                input.resultOrder match {
                    case Nil =>
                        // no order present
                        labeledExpr
                    case sortExprs =>
                        // need to order again
                        // because projection could be materialized and
                        // may not preserve input order
                        RelOpExpr(Order(sortExprs), List(labeledExpr))
                }
        }
    }

    override def clone(
        newPredLabels: List[(ScalExpr, Label)]
    ): PredRowLabeler = new MatPredLabeler(newPredLabels)
}
