package com.google.cloud.teleport.it.bigquery;

import org.junit.Test;

import java.util.Arrays;

import static com.google.cloud.teleport.it.bigquery.BigQueryResourceManagerUtils.checkValidTableId;
import static org.junit.Assert.assertThrows;

public class BigQueryResourceManagerUtilsTest {

    @Test
    public void testCheckValidTableIdWhenIdIsTooShort() {
        assertThrows(IllegalArgumentException.class, () -> checkValidTableId(""));
    }

    @Test
    public void testCheckValidTableIdWhenIdIsTooLong() {
        char[] chars = new char[1025];
        Arrays.fill(chars, 'a');
        String s = new String(chars);
        assertThrows(IllegalArgumentException.class, () -> checkValidTableId(s));
    }
    @Test
    public void testCheckValidTableIdWhenIdContainsIllegalCharacter() {
        assertThrows(IllegalArgumentException.class, () -> checkValidTableId("table-id%"));
        assertThrows(IllegalArgumentException.class, () -> checkValidTableId("ta#ble-id"));
    }
}
