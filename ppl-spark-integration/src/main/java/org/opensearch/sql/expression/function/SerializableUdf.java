/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import scala.Function2;
import scala.Serializable;
import scala.runtime.AbstractFunction2;


public class SerializableUdf {

    //this class should not have any fields, only static methods and static classes

    private SerializableUdf() {

    }

    public static Function2<String,String,Boolean> cidrFunction = new SerializableAbstractFunction2<>() {

        @Override
        public Boolean apply(String ipAddress, String cidrBlock) {
            //TODO implement ip address validation and cidr validation for ipv4 and ipv6
            return Boolean.FALSE;
        }
    };

    static abstract public class SerializableAbstractFunction2<T1,T2,R> extends AbstractFunction2<T1,T2,R>
            implements Serializable {
    }
}
