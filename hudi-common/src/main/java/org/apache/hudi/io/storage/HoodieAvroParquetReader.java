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

import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class HoodieAvroParquetReader implements HoodieAvroFileReader {

  private final Path path;
  private final Configuration conf;
  private final BaseFileUtils parquetUtils;
  private List<ParquetReaderIterator> readerIterators = new ArrayList<>();

  public HoodieAvroParquetReader(Configuration configuration, Path path) {
    this.conf = configuration;
    this.path = path;
    this.parquetUtils = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
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
  public ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema schema) throws IOException {
    return getIndexedRecordIteratorInternal(schema, Option.empty());
  }

  @Override
  public ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
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
    AvroReadSupport.setAvroReadSchema(conf, schema);
    if (requestedSchema.isPresent()) {
      AvroReadSupport.setRequestedProjection(conf, requestedSchema.get());
    }
    // If we put complex type at end, we must set false when we read with AvroRecord and write with SparkRecord
    // The old parquet log written with old list can be read by new list schema
    conf.set(WRITE_OLD_LIST_STRUCTURE, "false");
    ParquetReader<IndexedRecord> reader = AvroParquetReader.<IndexedRecord>builder(path).withConf(conf).build();
    ParquetReaderIterator<IndexedRecord> parquetReaderIterator = new ParquetReaderIterator<>(reader);
    readerIterators.add(parquetReaderIterator);
    return parquetReaderIterator;
  }
}
