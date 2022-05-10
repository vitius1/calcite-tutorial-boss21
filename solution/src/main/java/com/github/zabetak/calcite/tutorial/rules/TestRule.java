package com.github.zabetak.calcite.tutorial.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.TransformationRule;

public class TestRule extends AggregateReduceFunctionsRule implements TransformationRule {
    protected TestRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall ruleCall) {
        Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];

    }

    public interface Config extends AggregateReduceFunctionsRule.Config {
        TestRule.Config DEFAULT = EMPTY.as(TestRule.Config.class)
                .withOperandFor(Aggregate.class)
                .withDescription("Vitek Group by")
                .as(TestRule.Config.class);

        @Override default TestRule toRule() {
            return new TestRule(this);
        }
    }
}
