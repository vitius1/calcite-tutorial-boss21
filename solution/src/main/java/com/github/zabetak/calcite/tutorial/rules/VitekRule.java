package com.github.zabetak.calcite.tutorial.rules;

import com.github.zabetak.calcite.tutorial.DataType.BasicSqlTypeUnique;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import com.github.zabetak.calcite.tutorial.indexer.TpchTable;
import com.google.common.collect.Lists;

import java.util.List;


public class VitekRule extends ReduceRule<VitekRule.Config> implements SubstitutionRule {
  VitekRule(Config config) {
    super(config);
  }

  public boolean IsUnique(RexNode operand, List<RelDataTypeField> allFields) {
    int index = Integer.parseInt(operand.toString().replace("$", ""));
    RelDataTypeField field = allFields.get(index);
    RelDataType a = field.getValue();

    for (TpchTable table : TpchTable.values()) {
      for (TpchTable.Column column : table.columns) {
        if(field.getName()==column.name) {
          if(column.unique) {
            return true;
          } else {
            return false;
          }
        }
      }
    }
    return false;
  }

  public boolean IsNullable(RexNode operand, List<RelDataTypeField> allFields) {
    int index = Integer.parseInt(operand.toString().replace("$", ""));
    RelDataTypeFieldImpl field = (RelDataTypeFieldImpl) allFields.get(index);
    BasicSqlTypeUnique a = (BasicSqlTypeUnique)field.getType();
    return a.isUnique;
  }


  public RexNode IsDuplicate(RexNode conditions) {
    RexNode o1 = ((RexCall) conditions).getOperands().get(0);
    RexNode o2 = ((RexCall) conditions).getOperands().get(1);

    RexNode o11 = ((RexCall) o1).getOperands().get(0);
    RexNode o12 = ((RexCall) o1).getOperands().get(1);

    RexNode o21 = ((RexCall) o2).getOperands().get(0);
    RexNode o22 = ((RexCall) o2).getOperands().get(1);
    if(o11.equals(o12) && o21.equals(o22)) {
      return null;
    } else if(o11.equals(o12)){
      return o2;
    } else if(o21.equals(o22)){
      return o1;
    }
    return conditions;
  }

  public RexNode Reduce (RexNode conditions, List<RelDataTypeField> allFields) {
    if(conditions.nodeCount()<=3) {
      return conditions;
    }
    conditions=IsDuplicate(conditions);
    RexNode operand1=((RexCall) conditions).getOperands().get(0);
    RexNode operand2=((RexCall) conditions).getOperands().get(1);
    SqlKind kind = operand1.getKind();
    if(kind.sql=="INPUT_REF") {
      IsNullable(operand1, allFields);
      if(IsUnique(operand1, allFields)) {
        // something happaned

      }
    }

    if(conditions.nodeCount()<=3) {
      return conditions;
    }
    for(RexNode operand : ((RexCall) conditions).getOperands()) {
      kind = operand.getKind();
      if(kind.sql=="INPUT_REF") {
        operand1=((RexCall) operand).getOperands().get(0);
        operand2=((RexCall) operand).getOperands().get(1);
        IsNullable(operand1, allFields);
        if(IsUnique(operand1, allFields)) {
          // something happaned
        }
      } else if (kind.sql=="LITERAL") {
        // something happaned
      } else {
        operand=Reduce(operand, allFields);
      }
      operand=IsDuplicate(operand);
    }
    return conditions;
  }

  // rule
  @Override public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    //final RelMetadataQuery mq = call.getMetadataQuery();
    List<RexNode> expList =
        Lists.newArrayList(filter.getCondition());
    RexNode Conditions = expList.get(0);
    //RexNodeUnique Conditions2 = (RexNodeUnique) Conditions;






    // all fields from sql query
    RelDataType input = filter.getInput().getRowType();
    //RelDataTypeUnique input2 = (RelDataTypeUnique) input;
    List<RelDataTypeField> allFields = input.getFieldList();
    if (allFields instanceof RelDataTypeImpl) {
      int asd = 1;
    }

    RexNode a = ((RexCall) Conditions).getOperands().get(0);

    RexNode reduced = Reduce(Conditions, allFields);
    //RexNode reduced = Conditions;

    if(reduced.equals(Conditions)) {

    } else {
      if(reduced.isAlwaysTrue()) {
        call.transformTo(
                filter.getInput());
      }
      else {
        call.transformTo(call.builder()
                .push(filter.getInput())
                .filter(expList).build());
      }
      call.getPlanner().prune(filter);
    }


  }


  public interface Config extends ReduceRule.Config {
    VitekRule.Config DEFAULT = EMPTY.as(Config.class)
        .withMatchNullability(true)
        .withOperandFor(LogicalFilter.class)
        .withDescription("Vitek")
        .as(VitekRule.Config.class);

    @Override default VitekRule toRule() {
      return new VitekRule(this);
    }
  }
}
