package com.google.cloud.teleport.it.common;

import com.google.re2j.Pattern;
import org.junit.Test;

import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateInstanceId;
import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateNewId;
import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

public class ResourceManagerUtilsTest {

    protected static final int MAX_BASE_ID_LENGTH = 30;
    protected static final Pattern ILLEGAL_INSTANCE_CHARS = Pattern.compile("[\\W_]");

    @Test
    public void testGenerateInstanceIdShouldReplaceDollarSignWithHyphen() {
        String testBaseString = "test$instance";

        String actual = generateInstanceId(testBaseString, ILLEGAL_INSTANCE_CHARS, MAX_BASE_ID_LENGTH);

        assertThat(actual).matches("test-instance-\\d{8}-\\d{6}-\\d{6}");
    }

    @Test
    public void testGenerateInstanceIdShouldReplaceDotWithHyphen() {
        String testBaseString = "test.instance";

        String actual = generateInstanceId(testBaseString, ILLEGAL_INSTANCE_CHARS, MAX_BASE_ID_LENGTH);

        assertThat(actual).matches("test-instance-\\d{8}-\\d{6}-\\d{6}");
    }

    @Test
    public void testGenerateInstanceIdShouldReplaceNonLetterFirstCharWithLetter() {
        String testBaseString = "0-test-instance";

        String actual = generateInstanceId(testBaseString, ILLEGAL_INSTANCE_CHARS, MAX_BASE_ID_LENGTH);

        assertThat(actual).matches("[a-z]-test-instance-\\d{8}-\\d{6}-\\d{6}");
    }

    @Test
    public void testGenerateInstanceIdShouldReplaceUnderscoreWithHyphen() {
        String testBaseString = "test_instance";

        String actual = generateInstanceId(testBaseString, ILLEGAL_INSTANCE_CHARS, MAX_BASE_ID_LENGTH);

        assertThat(actual).matches("test-instance-\\d{8}-\\d{6}-\\d{6}");
    }

    @Test
    public void testGenerateInstanceIdShouldReplaceUpperCaseLettersWithLowerCase() {
        String testBaseString = "Test-Instance";

        String actual = generateInstanceId(testBaseString, ILLEGAL_INSTANCE_CHARS, MAX_BASE_ID_LENGTH);

        assertThat(actual).matches("test-instance-\\d{8}-\\d{6}-\\d{6}");
    }

    @Test
    public void testGenerateInstanceIdShouldThrowErrorWithEmptyInput() {
        String testBaseString = "";

        assertThrows(IllegalArgumentException.class, () -> generateInstanceId(testBaseString, ILLEGAL_INSTANCE_CHARS, MAX_BASE_ID_LENGTH));
    }

    @Test
    public void testGenerateNewIdShouldReturnNewIdWhenInputLengthIsLongerThanTargetLength() {
        String longId = "long-test-id-string";

        String actual = generateNewId(longId, 13);

        assertThat(actual).matches("long-([a-zA-Z0-9]){8}");
    }

    @Test
    public void testGenerateNewIdShouldReturnOldIdWhenInputLengthIsNotLongerThanTargetLength() {
        String shortId = "test-id";

        String actual = generateNewId(shortId, shortId.length());

        assertThat(actual).isEqualTo(shortId);
    }

    @Test
    public void testGenerateNewIdShouldThrowExceptionWhenTargetLengthIsNotGreaterThanEight() {
        String id = "long-test-id";

        assertThrows(IllegalArgumentException.class, () -> generateNewId(id, 8));
    }
}
