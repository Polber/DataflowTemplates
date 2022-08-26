/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.bigquery;

import com.google.re2j.Pattern;

import static com.google.cloud.teleport.it.common.ResourceManagerUtils.generateResourceId;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Utilities for {@link com.google.cloud.teleport.it.bigquery.BigQueryResourceManager}
 * implementations.
 */
public final class BigQueryResourceManagerUtils {
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private static final int MAX_DATASET_ID_LENGTH = 1024;
    private static final Pattern ILLEGAL_DATASET_ID_CHARS = Pattern.compile("[^a-zA-Z0-9_]");
    private static final int MIN_TABLE_ID_LENGTH = 1;
    private static final int MAX_TABLE_ID_LENGTH = 1024;
    private static final Pattern ILLEGAL_TABLE_CHARS = Pattern.compile("[^a-zA-Z0-9-_]");

    private BigQueryResourceManagerUtils() {}

    /**
     * Utility function to generate a formatted dataset name.
     *
     * <p>A dataset name must contain only alphanumeric characters and underscores with a max length of 1024.</p>
     *
     * @param datasetName the original given dataset name to be formatted
     * @return a BigQuery compatible dataset name.
     */
    static String generateDatasetId(String datasetName) {
        // Letters, numbers, and underscores only allowed. max length of 1024. Add check for these
//        checkArgument(datasetName.length() != 0, "baseString cannot be empty!");
//        String illegalCharsRemoved = ILLEGAL_DATASET_ID_CHARS.matcher(datasetName).replaceAll("_");
//
//        LocalDateTime localDateTime = LocalDateTime.now(ZoneId.of("UTC"));
//
//        String datasetNameAppend = "_" + localDateTime.format(DATE_FORMAT) + "_" + System.nanoTime();
//        String baseDatasetName = illegalCharsRemoved.substring(0, Math.min(illegalCharsRemoved.length(), MAX_DATASET_ID_LENGTH - datasetNameAppend.length()));
//
//        return baseDatasetName + datasetNameAppend;

        return generateResourceId(datasetName, ILLEGAL_DATASET_ID_CHARS, "_", MAX_DATASET_ID_LENGTH);
    }

    /**
     * Checks whether the given table ID is valid according to GCP constraints.
     *
     * @param idToCheck the table ID to check.
     * @throws IllegalArgumentException if the table ID is invalid.
     */
    static void checkValidTableId(String idToCheck) {
        if (idToCheck.length() < MIN_TABLE_ID_LENGTH) {
            throw new IllegalArgumentException("Table ID " + idToCheck + " cannot be empty. ");
        }
        if (idToCheck.length() > MAX_TABLE_ID_LENGTH) {
            throw new IllegalArgumentException("Table ID " + idToCheck + " cannot be longer than " + MAX_TABLE_ID_LENGTH + " characters.");
        }
        if (ILLEGAL_TABLE_CHARS.matcher(idToCheck).find()) {
            throw new IllegalArgumentException("Table ID " + idToCheck + " is not a valid ID. Only letters, numbers, hyphens and underscores are allowed.");
        }
    }
}