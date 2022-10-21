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

package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.Option;

import java.io.IOException;

/**
 * Default payload used in HoodieAvroRecord. We dont need to serialize/deserialize avro record in payload multiple times.
 */
public class HoodieAvroInsertValuePayload implements HoodieRecordPayload<RewriteAvroPayload> {

  private Option<IndexedRecord> record;

  public HoodieAvroInsertValuePayload(Option<IndexedRecord> record) {
    this.record = record;
  }

  @Override
  public RewriteAvroPayload preCombine(RewriteAvroPayload another) {
    throw new UnsupportedOperationException("precombine is not expected for HoodieAvroInsertValuePayload");
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    throw new UnsupportedOperationException("combineAndGetUpdateValue is not expected for HoodieAvroInsertValuePayload");
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    return record;
  }
}
