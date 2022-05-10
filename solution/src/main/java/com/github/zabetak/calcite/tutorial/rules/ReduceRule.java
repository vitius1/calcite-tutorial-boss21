package com.github.zabetak.calcite.tutorial.rules;

import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.util.ImmutableBeans;


public abstract class ReduceRule<C extends ReduceRule.Config> extends RelRule<C>
        implements SubstitutionRule {
    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected ReduceRule(C config) {
        super(config);
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        @Override ReduceRule<?> toRule();

        /** Whether to add a CAST when a nullable expression
         * reduces to a NOT NULL literal. */
        @ImmutableBeans.Property
        @ImmutableBeans.BooleanDefault(false)
        boolean matchNullability();

        /** Sets {@link #matchNullability()}. */
        ReduceRule.Config withMatchNullability(boolean matchNullability);

        /** Defines an operand tree for the given classes. */
        default ReduceRule.Config withOperandFor(Class<? extends LogicalFilter> relClass) {
            return withOperandSupplier(b -> b.operand(relClass).anyInputs())
                    .as(ReduceRule.Config.class);
        }
    }
}
