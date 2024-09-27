package org.opensearch.sql.expression.function;

import org.junit.Assert;
import org.junit.Test;

public class SerializableUdfTest {

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

    @Test
    public void cidrBasicTest() {
        Assert.assertTrue(SerializableUdf.cidrFunction.apply("192.168.0.0", "192.168.0.0/24"));
        Assert.assertFalse(SerializableUdf.cidrFunction.apply("10.10.0.0", "192.168.0.0/24"));
        Assert.assertTrue(SerializableUdf.cidrFunction.apply("2001:0db8:85a3:0000:0000:8a2e:0370:7334", "2001:db8::/32"));
        Assert.assertFalse(SerializableUdf.cidrFunction.apply("2001:0db7:85a3:0000:0000:8a2e:0370:7334", "2001:0db8::/32"));
    }
}
