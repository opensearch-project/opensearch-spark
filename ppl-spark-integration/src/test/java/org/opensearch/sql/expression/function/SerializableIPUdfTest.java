/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.expression.function;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SerializableIPUdfTest {

    @Test(expected = RuntimeException.class)
    public void cidrNullIpTest() {
        SerializableUdf.cidrFunction.apply(null, "192.168.0.0/24");
    }

    @Test(expected = RuntimeException.class)
    public void cidrEmptyIpTest() {
        SerializableUdf.cidrFunction.apply("", "192.168.0.0/24");
    }

    @Test(expected = RuntimeException.class)
    public void cidrNullCidrTest() {
        SerializableUdf.cidrFunction.apply("192.168.0.0", null);
    }

    @Test(expected = RuntimeException.class)
    public void cidrEmptyCidrTest() {
        SerializableUdf.cidrFunction.apply("192.168.0.0", "");
    }

    @Test(expected = RuntimeException.class)
    public void cidrInvalidIpTest() {
        SerializableUdf.cidrFunction.apply("xxx", "192.168.0.0/24");
    }

    @Test(expected = RuntimeException.class)
    public void cidrInvalidCidrTest() {
        SerializableUdf.cidrFunction.apply("192.168.0.0", "xxx");
    }
    
    @Test(expected = RuntimeException.class)
    public void cirdMixedIpVersionTest() {
        SerializableUdf.cidrFunction.apply("2001:0db8:85a3:0000:0000:8a2e:0370:7334", "192.168.0.0/24");
        SerializableUdf.cidrFunction.apply("192.168.0.0", "2001:db8::/324");
    }

    @Test(expected = RuntimeException.class)
    public void cirdMixedIpVersionTestV6V4() {
        SerializableUdf.cidrFunction.apply("2001:0db8:85a3:0000:0000:8a2e:0370:7334", "192.168.0.0/24");
    }

    @Test(expected = RuntimeException.class)
    public void cirdMixedIpVersionTestV4V6() {
        SerializableUdf.cidrFunction.apply("192.168.0.0", "2001:db8::/324");
    }

    @Test
    public void cidrBasicTest() {
        Assert.assertTrue(SerializableUdf.cidrFunction.apply("192.168.0.0", "192.168.0.0/24"));
        Assert.assertFalse(SerializableUdf.cidrFunction.apply("10.10.0.0", "192.168.0.0/24"));
        Assert.assertTrue(SerializableUdf.cidrFunction.apply("2001:0db8:85a3:0000:0000:8a2e:0370:7334", "2001:db8::/32"));
        Assert.assertFalse(SerializableUdf.cidrFunction.apply("2001:0db7:85a3:0000:0000:8a2e:0370:7334", "2001:0db8::/32"));
    }
}
