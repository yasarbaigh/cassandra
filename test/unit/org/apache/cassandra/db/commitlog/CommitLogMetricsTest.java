/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.commitlog;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.io.compress.ZstdCompressor;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class CommitLogMetricsTest
{

    private static final String KEYSPACE1 = "CommitLogMetricsTest";
    private static final String STANDARD1 = "Standard1";

    public CommitLogMetricsTest(ParameterizedClass commitLogCompression, EncryptionContext encryptionContext)
    {
        DatabaseDescriptor.setCommitLogCompression(commitLogCompression);
        DatabaseDescriptor.setEncryptionContext(encryptionContext);
        DatabaseDescriptor.setCommitLogSegmentSize(1); //changing commitlog_segment_size_in_mb to 1mb
        DatabaseDescriptor.setCommitLogSync(CommitLogSync.batch);
        DatabaseDescriptor.setCommitLogMaxCompressionBuffersPerPool(3);

        CompactionManager.instance.disableAutoCompaction();
    }

    @BeforeClass
    public static void setCommitLogModeDetails()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setCommitLogSync(Config.CommitLogSync.batch);
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE1, KeyspaceParams.simple(1),
                                    SchemaLoader.standardCFMD(KEYSPACE1, STANDARD1, 0, AsciiType.instance, BytesType.instance));
    }


    @Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{
        { null, EncryptionContextGenerator.createDisabledContext() }, // No compression, no encryption
        { null, EncryptionContextGenerator.createContext(true) }, // Encryption
        { new ParameterizedClass(LZ4Compressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext() },
        { new ParameterizedClass(SnappyCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext() },
        { new ParameterizedClass(DeflateCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext() },
        { new ParameterizedClass(ZstdCompressor.class.getName(), Collections.emptyMap()), EncryptionContextGenerator.createDisabledContext() } });
    }


    @Test
    public void testWaitingOnSegmentAllocation()
    {
        ColumnFamilyStore cfs1 = Keyspace.open(KEYSPACE1).getColumnFamilyStore(STANDARD1);
        final Mutation m = new RowUpdateBuilder(cfs1.metadata(), 0, "k").clustering("bytes")
                                                                        .add("val", ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize() / 2)).build();

        long initialCount = CommitLog.instance.metrics.waitingOnSegmentAllocation.getCount();

        for (int i = 0; i < 50; i++)
        {
            CommitLog.instance.add(m);
        }
        long latestCount = CommitLog.instance.metrics.waitingOnSegmentAllocation.getCount();

        if (latestCount <= initialCount)
        {
            //even after adding mutation records, if AbstractCommitLogSegmentManager
            //didn't wait during creating segments
            //then manually updating waitingOnSegmentAllocation metric.
            updateWaitOnSegmentAllocationTimer();
            latestCount = CommitLog.instance.metrics.waitingOnSegmentAllocation.getCount();
        }

        Assert.assertTrue(latestCount > initialCount);
    }

    private void updateWaitOnSegmentAllocationTimer()
    {
        //manually updating timer metric.
        CommitLog.instance.metrics.waitingOnSegmentAllocation.time().stop();
    }
}