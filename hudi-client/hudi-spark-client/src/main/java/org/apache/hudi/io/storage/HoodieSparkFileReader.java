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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.MappingIterator;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.function.Function;

public interface HoodieSparkFileReader extends HoodieFileReader<InternalRow> {

  ClosableIterator<InternalRow> getInternalRowIterator(Schema readerSchema) throws IOException;

  default ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(Schema readerSchema, Map<String, Object> prop) throws IOException {
    return new MappingIterator<>(getInternalRowIterator(readerSchema), createMapper(readerSchema, prop));
  }

  static Function<InternalRow, HoodieRecord<InternalRow>> createMapper(Schema schema, Map<String, Object> prop) {
    Class<?> utils = ReflectionUtils.getClass("org.apache.spark.sql.hudi.HoodieSparkRecordUtils");
    Method createMapper = null;
    try {
      createMapper = utils.getMethod("createMapper", Schema.class, Map.class);
      return (Function<InternalRow, HoodieRecord<InternalRow>>) createMapper.invoke(null, schema, prop);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new HoodieException(e);
    }
  }
}
