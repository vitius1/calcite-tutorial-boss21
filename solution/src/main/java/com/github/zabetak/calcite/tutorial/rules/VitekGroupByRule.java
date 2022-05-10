package com.github.zabetak.calcite.tutorial.rules;
import com.github.zabetak.calcite.tutorial.DataType.BasicSqlTypeUnique;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mappings;
import java.util.*;


public class VitekGroupByRule extends GroupByRule<VitekGroupByRule.Config>
        implements TransformationRule {


    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected VitekGroupByRule(Config config) {
        super(config);
    }

    private boolean Reducible(List<RelDataTypeField> allColumns, List<Integer> listGroupByColumnId) {
        for (int columnId : listGroupByColumnId) {
            BasicSqlTypeUnique type = (BasicSqlTypeUnique) allColumns.get(columnId).getType();
            if (type.isUnique) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final Project selectProject = call.rel(0);
        final Aggregate aggregate = call.rel(1);
        final Project aggregateProject = call.rel(2);
        final TableScan table = call.rel(3);
        final List<RexNode> selectExpList =
                Lists.newArrayList(selectProject.getProjects());
        final List<RelDataTypeField> allFields = aggregateProject.getRowType().getFieldList();
        final List<Integer> listGroupById = aggregate.getGroupSet().asList();

        if(Reducible(allFields, listGroupById)) {
            List<AggregateCall> AggCalls = aggregate.getAggCallList();
            for (AggregateCall aggCall:AggCalls) {
                List<Integer> argList = aggCall.getArgList();
                if(argList.size()>1) {
                    return;
                }
                int arg = 0;
                if(argList.size()<1) {
                    String name = aggCall.getName();
                    if(name.contains("EXPR$")) {
                        arg = Integer.parseInt(name.split(java.util.regex.Pattern.quote("EXPR$"))[1]);
                    } else {
                        return;
                    }
                } else {
                    arg = argList.get(0);
                }
                SqlKind sqlKind = aggCall.getAggregation().getKind();


                //how to create reldatatype
                //RelDataType type2 = new BasicSqlTypeUnique(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL);
                //RelDataType typeProject = selectProject.getRowType().getFieldList().get(arg).getType();
                RelDataType fieldtype = allFields.get(arg).getType();
                RelDataType type = selectExpList.get(arg).getType();

                switch(sqlKind) {
                    case SUM:
                        if(!fieldtype.equals(type)) {
                            RexNode rexnode = rexBuilder.makeLiteral(1, type);
                            selectExpList.set(arg, rexnode);
                        }
                        break;
                    case COUNT:
                        RexNode rexnode = rexBuilder.makeLiteral(1, type);
                        selectExpList.set(arg, rexnode);
                        break;
                    default:
                        return;
                }
            }
            /*
            MAPPING TEST
            Mapping map1 = Mappings.create(MappingType.INVERSE_SURJECTION, 9, 3);
            map1.set(0, 0);
            map1.set(1, 1);
            map1.set(3, 2);
            Mappings.TargetMapping map2 = aggregateProject.getMapping();
             */

            if(selectProject.getMapping()==null) {
                return;
            }

            relBuilder.push(table);
            relBuilder.project(
                    RexPermuteInputsShuttle.of(selectProject.getMapping()).visitList(selectExpList),
                    selectProject.getRowType().getFieldNames());
            RelNode pr = relBuilder.build();
            call.transformTo(pr);
            call.getPlanner().prune(pr);
        }
    }

    public interface Config extends GroupByRule.Config {
        VitekGroupByRule.Config DEFAULT = EMPTY.as(Config.class)
                .withOperandFor(LogicalAggregate.class)
                .withDescription("Vitek Group by")
                .as(VitekGroupByRule.Config.class);
        @Override default VitekGroupByRule toRule() {
            return new VitekGroupByRule(this);
        }
    }
}
