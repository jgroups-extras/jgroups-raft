package org.jgroups.tests;

import org.jgroups.Global;
import org.testng.annotations.Test;

import static org.jgroups.util.IntegerHelper.fromByteArrayToInt;
import static org.jgroups.util.IntegerHelper.fromIntToByteArray;

/**
 * Created by ugol on 05/12/14.
 */

@Test(groups= Global.FUNCTIONAL,singleThreaded=true)
public class IntegerHelperTest {

    public void testZeroConversion() {
        assert 0 == convertToBytesAndBack(0);
    }

    public void testPositiveConversion() {
        assert 42 == convertToBytesAndBack(42);
    }

    public void testMaxConversion() {
        assert Integer.MAX_VALUE == convertToBytesAndBack(Integer.MAX_VALUE);
    }

    public void testNegativeConversion() {
        assert -42 == convertToBytesAndBack(-42);
    }

    public void testMinConversion() {
        assert Integer.MIN_VALUE == convertToBytesAndBack(Integer.MIN_VALUE);
    }


    private int convertToBytesAndBack(int number) {
        byte[] b = fromIntToByteArray(number);
        return fromByteArrayToInt(b);
    }

}