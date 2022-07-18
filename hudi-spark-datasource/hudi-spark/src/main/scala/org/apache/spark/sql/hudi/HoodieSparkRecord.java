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

package org.apache.spark.sql.hudi;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.BaseKeyGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.sql.types.DataTypes.BooleanType;

/**
 * Spark Engine-specific Implementations of `HoodieRecord`.
 */
public class HoodieSparkRecord extends HoodieRecord<InternalRow> {

  public HoodieSparkRecord(HoodieKey key, InternalRow data, Comparable orderingVal) {
    super(key, data, orderingVal);
  }

  public HoodieSparkRecord(HoodieKey key, InternalRow data, HoodieOperation operation, Comparable orderingVal) {
    super(key, data, operation, orderingVal);
  }

  public HoodieSparkRecord(HoodieRecord<InternalRow> record) {
    super(record);
  }

  public HoodieSparkRecord() {
  }

  @Override
  public HoodieRecord<InternalRow> newInstance() {
    return new HoodieSparkRecord(this);
  }

  @Override
  public HoodieRecord<InternalRow> newInstance(HoodieKey key, HoodieOperation op) {
    return new HoodieSparkRecord(key, data, op, getOrderingValue());
  }

  @Override
  public HoodieRecord<InternalRow> newInstance(HoodieKey key) {
    return new HoodieSparkRecord(key, data, getOrderingValue());
  }

  @Override
  public String getRecordKey(Option<BaseKeyGenerator> keyGeneratorOpt) {
    return getRecordKey();
  }

  @Override
  public String getRecordKey(String keyFieldName) {
    return getRecordKey();
  }

  @Override
  public HoodieRecord mergeWith(HoodieRecord other, Schema readerSchema, Schema writerSchema) throws IOException {
    StructType readerStructType = HoodieInternalRowUtils.getCachedSchema(readerSchema);
    StructType writerStructType = HoodieInternalRowUtils.getCachedSchema(writerSchema);
    InternalRow mergeRow = HoodieInternalRowUtils.stitchRecords(data, readerStructType, (InternalRow) other.getData(), readerStructType, writerStructType);
    return new HoodieSparkRecord(getKey(), mergeRow, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Schema targetSchema, TypedProperties props) throws IOException {
    StructType readerStructType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    StructType targetStructType = HoodieInternalRowUtils.getCachedSchema(targetSchema);
    InternalRow rewriteRow = HoodieInternalRowUtils.rewriteRecord(data, readerStructType, targetStructType);
    return new HoodieSparkRecord(getKey(), rewriteRow, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecord(Schema recordSchema, Properties prop, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields) throws IOException {
    StructType readerStructType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    StructType writeSchemaWithMetaFieldsStructType = HoodieInternalRowUtils.getCachedSchema(writeSchemaWithMetaFields);
    InternalRow rewriteRow = schemaOnReadEnabled ? HoodieInternalRowUtils.rewriteRecordWithNewSchema(data, readerStructType, writeSchemaWithMetaFieldsStructType, new HashMap<>())
        : HoodieInternalRowUtils.rewriteRecord(data, readerStructType, writeSchemaWithMetaFieldsStructType);
    return new HoodieSparkRecord(getKey(), rewriteRow, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithMetadata(Schema recordSchema, Properties prop, boolean schemaOnReadEnabled, Schema writeSchemaWithMetaFields, String fileName) throws IOException {
    StructType readerStructType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    StructType writeSchemaWithMetaFieldsStructType = HoodieInternalRowUtils.getCachedSchema(writeSchemaWithMetaFields);
    InternalRow rewriteRow = schemaOnReadEnabled ? HoodieInternalRowUtils.rewriteEvolutionRecordWithMetadata(data, readerStructType, writeSchemaWithMetaFieldsStructType, fileName)
        : HoodieInternalRowUtils.rewriteRecordWithMetadata(data, readerStructType, writeSchemaWithMetaFieldsStructType, fileName);
    return new HoodieSparkRecord(getKey(), rewriteRow, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties prop, Schema newSchema, Map<String, String> renameCols) throws IOException {
    StructType readerStructType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    StructType newStructType = HoodieInternalRowUtils.getCachedSchema(newSchema);
    InternalRow rewriteRow = HoodieInternalRowUtils.rewriteRecordWithNewSchema(data, readerStructType, newStructType, renameCols);
    return new HoodieSparkRecord(getKey(), rewriteRow, getOperation());
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties prop, Schema newSchema, Map<String, String> renameCols, Mapper mapper) throws IOException {
    StructType readerStructType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    StructType newStructType = HoodieInternalRowUtils.getCachedSchema(newSchema);
    InternalRow rewriteRow = HoodieInternalRowUtils.rewriteRecordWithNewSchema(data, readerStructType, newStructType, renameCols);
    // TODO change mapper type
    return mapper.apply((IndexedRecord) rewriteRow);
  }

  @Override
  public HoodieRecord rewriteRecordWithNewSchema(Schema recordSchema, Properties prop, Schema newSchema) throws IOException {
    StructType readerStructType = HoodieInternalRowUtils.getCachedSchema(recordSchema);
    StructType newStructType = HoodieInternalRowUtils.getCachedSchema(newSchema);
    InternalRow rewriteRow = HoodieInternalRowUtils.rewriteRecord(data, readerStructType, newStructType);
    return new HoodieSparkRecord(getKey(), rewriteRow, getOperation());
  }

  @Override
  public HoodieRecord overrideMetadataFieldValue(Schema recordSchema, Properties prop, int pos, String newValue) throws IOException {
    data.update(pos, newValue);
    return this;
  }

  @Override
  public HoodieRecord addMetadataValues(Schema recordSchema, Properties prop, Map<HoodieMetadataField, String> metadataValues) throws IOException {
    Arrays.stream(HoodieMetadataField.values()).forEach(metadataField -> {
      String value = metadataValues.get(metadataField);
      if (value != null) {
        data.update(recordSchema.getField(metadataField.getFieldName()).pos(), value);
      }
    });
    return this;
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  @Override
  public boolean isPresent(Schema schema, Properties prop) throws IOException {
    if (null == data) {
      return false;
    }
    Object deleteMarker = data.get(schema.getField(HoodieRecord.HOODIE_IS_DELETED_FIELD).pos(), BooleanType);
    return !(deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }

  @Override
  public boolean shouldIgnore(Schema schema, Properties prop) throws IOException {
    // TODO SENTINEL should refactor without Avro(GenericRecord)
    if (null != data && data.equals(SENTINEL)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Option<IndexedRecord> toIndexedRecord(Schema schema, Properties prop) throws IOException {
    throw new UnsupportedOperationException();
  }
}