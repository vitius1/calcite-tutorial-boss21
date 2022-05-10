package com.github.zabetak.calcite.tutorial.rules;

import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.rules.SubstitutionRule;

public abstract class GroupByRule<C extends GroupByRule.Config> extends RelRule<C>
        implements SubstitutionRule {
    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected GroupByRule(C config) {
        super(config);
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        @Override
        GroupByRule<?> toRule();


        /** Defines an operand tree for the given classes. */
        default GroupByRule.Config withOperandFor(Class<? extends LogicalAggregate> relClass) {
            return withOperandSupplier(
                    b0 -> b0.operand(Project.class)
                            .oneInput(b1 ->
                            b1.operand(relClass)
                                    .oneInput(b2 ->
                                    b2.operand(Project.class)
                                            .oneInput(b3 ->
                                            b3.operand(TableScan.class).anyInputs()))))
                    .as(GroupByRule.Config.class);
        }
    }
}
