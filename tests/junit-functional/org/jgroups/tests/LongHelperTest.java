package org.jgroups.tests;

import org.jgroups.Global;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.jgroups.raft.util.LongHelper.fromByteArrayToLong;
import static org.jgroups.raft.util.LongHelper.fromLongToByteArray;


@Test(groups= Global.FUNCTIONAL,singleThreaded=true)
public class LongHelperTest {

    public void testNull() {
        Assert.assertEquals(0, fromByteArrayToLong(null));
    }

    public void testZeroConversion() {
        Assert.assertEquals(0, convertToBytesAndBack(0));
    }

    public void testPositiveConversion() {
        Assert.assertEquals(42, convertToBytesAndBack(42));
    }

    public void testMaxConversion() {
        Assert.assertEquals(Long.MAX_VALUE, convertToBytesAndBack(Long.MAX_VALUE));
    }

    public void testNegativeConversion() {
        Assert.assertEquals(-42, convertToBytesAndBack(-42));
    }

    public void testMinConversion() {
        Assert.assertEquals(Long.MIN_VALUE, convertToBytesAndBack(Long.MIN_VALUE));
    }


    private static long convertToBytesAndBack(long number) {
        byte[] b = fromLongToByteArray(number);
        return fromByteArrayToLong(b);
    }

}