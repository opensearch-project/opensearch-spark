/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;

public class MyAggregateCall extends RexCall {
    private AggregateCall call;

    protected MyAggregateCall(AggregateCall call) {
        super(call.getType(), call.getAggregation(), call.rexList);
        this.call = call;
    }

    public AggregateCall getCall() {
        return call;
    }
}
