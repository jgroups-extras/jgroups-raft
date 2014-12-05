package org.jgroups.tests;

import org.jgroups.Global;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.jgroups.util.IntegerHelper.fromByteArrayToInt;
import static org.jgroups.util.IntegerHelper.fromIntToByteArray;

/**
 * Created by ugol on 05/12/14.
 */

@Test(groups= Global.FUNCTIONAL,singleThreaded=true)
public class IntegerHelperTest {

    public void testZeroConversion() {
        Assert.assertEquals(0, convertToBytesAndBack(0));
    }

    public void testPositiveConversion() {
        Assert.assertEquals(42, convertToBytesAndBack(42));
    }

    public void testMaxConversion() {
        Assert.assertEquals(Integer.MAX_VALUE, convertToBytesAndBack(Integer.MAX_VALUE));
    }

    public void testNegativeConversion() {
        Assert.assertEquals(-42, convertToBytesAndBack(-42));
    }

    public void testMinConversion() {
        Assert.assertEquals(Integer.MIN_VALUE, convertToBytesAndBack(Integer.MIN_VALUE));
    }


    private int convertToBytesAndBack(int number) {
        byte[] b = fromIntToByteArray(number);
        return fromByteArrayToInt(b);
    }

}