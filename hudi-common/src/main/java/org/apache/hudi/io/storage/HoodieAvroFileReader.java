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

import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.RewriteAvroPayload;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.MapperUtils;
import org.apache.hudi.common.util.MappingIterator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.table.HoodieTableConfig.PAYLOAD_CLASS_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.PRECOMBINE_FIELD;
import static org.apache.hudi.common.util.MapperUtils.PARTITION_NAME;
import static org.apache.hudi.common.util.MapperUtils.POPULATE_META_FIELDS;
import static org.apache.hudi.common.util.MapperUtils.SIMPLE_KEY_GEN_FIELDS_OPT;
import static org.apache.hudi.common.util.MapperUtils.WITH_OPERATION_FIELD;

public interface HoodieAvroFileReader<T> extends HoodieFileReader<T>, AutoCloseable {

  ClosableIterator<IndexedRecord> getIndexedRecordIterator(Schema readerSchema) throws IOException;

  default Option<IndexedRecord> getRecordByKey(String key, Schema readerSchema) throws IOException {
    throw new UnsupportedOperationException();
  }

  default ClosableIterator<IndexedRecord> getRecordsByKeysIterator(List<String> keys, Schema schema) throws IOException {
    throw new UnsupportedOperationException();
  }

  default ClosableIterator<IndexedRecord> getRecordsByKeysIterator(List<String> keys) throws IOException {
    return getRecordsByKeysIterator(keys, getSchema());
  }

  default ClosableIterator<IndexedRecord> getRecordsByKeyPrefixIterator(List<String> keyPrefixes, Schema schema) throws IOException {
    throw new UnsupportedEncodingException();
  }

  default ClosableIterator<IndexedRecord> getRecordsByKeyPrefixIterator(List<String> keyPrefixes) throws IOException {
    return getRecordsByKeyPrefixIterator(keyPrefixes, getSchema());
  }

  default ClosableIterator<HoodieRecord<T>> getRecordsByKeysIterator(List<String> keys, Schema schema, Map<String, Object> prop) throws IOException {
    ClosableIterator<IndexedRecord> iterator = getRecordsByKeysIterator(keys, schema);
    return new MappingIterator<>(iterator, createMapper(prop));
  }

  default ClosableIterator<HoodieRecord<T>> getRecordsByKeyPrefixIterator(List<String> keyPrefixes, Schema schema, Map<String, Object> prop) throws IOException {
    ClosableIterator<IndexedRecord> iterator = getRecordsByKeyPrefixIterator(keyPrefixes, schema);
    return new MappingIterator<>(iterator, createMapper(prop));
  }

  @Override
  default ClosableIterator<HoodieRecord<T>> getRecordIterator(Schema schema, Map<String, Object> prop) throws IOException {
    return new MappingIterator<>(getIndexedRecordIterator(schema), createMapper(prop));
  }

  @Override
  default Option<HoodieRecord<T>> getRecordByKey(String key, Schema readerSchema, Map<String, Object> prop) throws IOException {
    return getRecordByKey(key, readerSchema).map(createMapper(prop));
  }

  static <T> Function<IndexedRecord, HoodieRecord<T>> createMapper(Map<String, Object> prop) {
    if (!MapperUtils.hasInfo(prop)) {
      return (data) -> (HoodieRecord<T>) new HoodieAvroIndexedRecord(data);
    } else {
      Option<Pair<String, String>> keyGen = (Option<Pair<String, String>>) prop.getOrDefault(SIMPLE_KEY_GEN_FIELDS_OPT, Option.empty());
      String payloadClass = prop.get(PAYLOAD_CLASS_NAME.key()).toString();
      String preCombineField = prop.get(PRECOMBINE_FIELD.key()).toString();
      boolean withOperationField = Boolean.parseBoolean(prop.get(WITH_OPERATION_FIELD).toString());
      boolean populateMetaFields = Boolean.parseBoolean(prop.getOrDefault(POPULATE_META_FIELDS, false).toString());
      Option<String> partitionName = (Option<String>) prop.getOrDefault(PARTITION_NAME, Option.empty());
      if (preCombineField == null) {
        // Support JavaExecutionStrategy
        return (data) -> {
          GenericRecord record = (GenericRecord) data;
          String key = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          String partition = record.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
          HoodieKey hoodieKey = new HoodieKey(key, partition);

          HoodieRecordPayload avroPayload = new RewriteAvroPayload(record);
          HoodieRecord hoodieRecord = new HoodieAvroRecord(hoodieKey, avroPayload);
          return hoodieRecord;
        };
      } else if (populateMetaFields) {
        return (data) ->
            SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) data,
                payloadClass, preCombineField, withOperationField);
      } else if (keyGen.isPresent()) {
        return (data) ->
            SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) data,
                payloadClass, preCombineField, keyGen.get(), withOperationField, Option.empty());
      } else {
        return (data) ->
            SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) data,
                payloadClass, preCombineField, withOperationField, partitionName);
      }
    }
  }
}
