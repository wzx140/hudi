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

package org.apache.hudi.io.storage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.MappingIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

public class HoodieAvroParquetReader extends HoodieAvroFileReaderBase {

  private final Path path;
  private final Configuration conf;
  private final BaseFileUtils parquetUtils;
  private final List<ParquetReaderIterator> readerIterators = new ArrayList<>();

  public HoodieAvroParquetReader(Configuration configuration, Path path) {
    this.conf = configuration;
    this.path = path;
    this.parquetUtils = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
  }

  @Override
  public ClosableIterator<HoodieRecord<IndexedRecord>> getRecordIterator(Schema readerSchema) throws IOException {
    // TODO(HUDI-4588) remove after HUDI-4588 is resolved
    // NOTE: This is a workaround to avoid leveraging projection w/in [[AvroParquetReader]],
    //       until schema handling issues (nullability canonicalization, etc) are resolved
    ClosableIterator<IndexedRecord> iterator = getIndexedRecordIterator(readerSchema);
    return new MappingIterator<>(iterator, data -> unsafeCast(new HoodieAvroIndexedRecord(data)));
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    return parquetUtils.readMinMaxRecordKeys(conf, path);
  }

  @Override
  public BloomFilter readBloomFilter() {
    return parquetUtils.readBloomFilterFromMetadata(conf, path);
  }

  @Override
  public Set<String> filterRowKeys(Set<String> candidateRowKeys) {
    return parquetUtils.filterRowKeys(conf, path, candidateRowKeys);
  }

  @Override
  protected ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema schema) throws IOException {
    return getIndexedRecordIteratorInternal(schema, Option.empty());
  }

  @Override
  protected ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    return getIndexedRecordIteratorInternal(readerSchema, Option.of(requestedSchema));
  }

  @Override
  public Schema getSchema() {
    return parquetUtils.readAvroSchema(conf, path);
  }

  @Override
  public void close() {
    readerIterators.forEach(ParquetReaderIterator::close);
  }

  @Override
  public long getTotalRecords() {
    return parquetUtils.getRowCount(conf, path);
  }

  private ClosableIterator<IndexedRecord> getIndexedRecordIteratorInternal(Schema schema, Option<Schema> requestedSchema) throws IOException {
    if (!requestedSchema.isPresent()) {
      AvroReadSupport.setAvroReadSchema(conf, schema);
    } else {
      // Make record schema the same as requestedSchema(reader schema)
      AvroReadSupport.setAvroReadSchema(conf, requestedSchema.get());
      AvroReadSupport.setRequestedProjection(conf, requestedSchema.get());
    }
    ParquetReader<IndexedRecord> reader = AvroParquetReader.<IndexedRecord>builder(path).withConf(conf).build();
    ParquetReaderIterator<IndexedRecord> parquetReaderIterator = new ParquetReaderIterator<>(reader);
    readerIterators.add(parquetReaderIterator);
    return parquetReaderIterator;
  }
}
