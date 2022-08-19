package com.google.cloud.teleport.it.bigtable;

import com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.IOException;

import static com.google.cloud.teleport.it.bigtable.BigtableResourceManagerUtils.generateDefaultClusters;
import static org.junit.Assert.assertThrows;

/** Unit tests for {@link DefaultBigtableResourceManager}. */
@RunWith(JUnit4.class)
public class BigtableResourceManagerUtilsTest {
    @Rule
    public final MockitoRule mockito = MockitoJUnit.rule();

    private static final String TEST_ID = "test";
    private static final String PROJECT_ID = "test-project";
    private static final String ZONE = "us-central1-a";
    private DefaultBigtableResourceManager testManager;

    @Before
    public void setup() throws IOException {
        testManager = new DefaultBigtableResourceManager(TEST_ID, PROJECT_ID);
    }

    @Test
    public void testDefaultClustersShould() {

    }
}
