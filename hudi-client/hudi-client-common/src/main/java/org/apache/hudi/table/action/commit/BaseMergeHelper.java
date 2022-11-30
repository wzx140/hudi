/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.utils.MergingIterator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.queue.IteratorBasedQueueConsumer;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;

/**
 * Helper to read records from previous version of base file and run Merge.
 */
public abstract class BaseMergeHelper {

  /**
   * Read records from previous version of base file and merge.
   * @param table Hoodie Table
   * @param upsertHandle Merge Handle
   * @throws IOException in case of error
   */
  public abstract void runMerge(HoodieTable<?, ?, ?, ?> table, HoodieMergeHandle<?, ?, ?, ?> upsertHandle) throws IOException;

  /**
   * Create Parquet record iterator that provides a stitched view of record read from skeleton and bootstrap file.
   * Skeleton file is a representation of the bootstrap file inside the table, with just the bare bone fields needed
   * for indexing, writing and other functionality.
   */
  protected Iterator<HoodieRecord> getMergingIterator(HoodieTable<?, ?, ?, ?> table,
      HoodieMergeHandle<?, ?, ?, ?> mergeHandle,
      Path bootstrapFilePath,
      Iterator<HoodieRecord> recordIterator) throws IOException {
    Configuration bootstrapFileConfig = new Configuration(table.getHadoopConf());
    HoodieRecordType recordType = table.getConfig().getRecordMerger().getRecordType();
    HoodieFileReader<HoodieRecord> bootstrapReader =
        HoodieFileReaderFactory.getReaderFactory(recordType).getFileReader(bootstrapFileConfig, bootstrapFilePath);
    return new MergingIterator(recordIterator, (Iterator) bootstrapReader.getRecordIterator(),
        (left, right) -> left.joinWith(right, mergeHandle.getWriterSchemaWithMetaFields()));
  }

  /**
   * Consumer that dequeues records from queue and sends to Merge Handle.
   */
  protected static class UpdateHandler extends IteratorBasedQueueConsumer<HoodieRecord, Void> {

    private final HoodieMergeHandle upsertHandle;

    protected UpdateHandler(HoodieMergeHandle upsertHandle) {
      this.upsertHandle = upsertHandle;
    }

    @Override
    public void consumeOneRecord(HoodieRecord record) {
      upsertHandle.write(record);
    }

    @Override
    public void finish() {}

    @Override
    protected Void getResult() {
      return null;
    }
  }
}
