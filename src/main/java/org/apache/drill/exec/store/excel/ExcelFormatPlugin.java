package org.apache.drill.exec.store.excel;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.FormatPluginConfig;
import org.apache.drill.common.logical.StoragePluginConfig;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.RecordWriter;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.EasyFormatPlugin;
import org.apache.drill.exec.store.dfs.easy.EasyWriter;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

public class ExcelFormatPlugin extends EasyFormatPlugin<ExcelFormatPlugin.ExcelFormatConfig> {

  private static final boolean IS_COMPRESSIBLE = false;

  private static final String DEFAULT_NAME = "excel";

  private ExcelFormatConfig config;

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcelFormatPlugin.class);

  public ExcelFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig storageConfig) {
    this(name, context, fsConf, storageConfig, new ExcelFormatConfig());
  }

  public ExcelFormatPlugin(String name, DrillbitContext context, Configuration fsConf, StoragePluginConfig config, ExcelFormatConfig formatPluginConfig) {
    super(name, context, fsConf, config, formatPluginConfig, true, false, false, IS_COMPRESSIBLE, formatPluginConfig.getExtensions(), DEFAULT_NAME);
    this.config = formatPluginConfig;
  }

  @Override
  public RecordReader getRecordReader(FragmentContext context, DrillFileSystem dfs, FileWork fileWork, List<SchemaPath> columns, String userName) throws ExecutionSetupException {
    return new ExcelRecordReader(context, fileWork.getPath(), dfs, columns, config);
  }


  @Override
  public int getReaderOperatorType() {
    return UserBitShared.CoreOperatorType.JSON_SUB_SCAN_VALUE;
  }

  @Override
  public int getWriterOperatorType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean supportsPushDown() {
    return true;
  }

  @Override
  public RecordWriter getRecordWriter(FragmentContext context, EasyWriter writer) throws IOException {
    return null;
  }

  @JsonTypeName("excel")
  public static class ExcelFormatConfig implements FormatPluginConfig {
    public List<String> extensions;

    public int headerRow = 0;

    public int lastRow = 1000000;

    public int sheetNumber = 0;

    public int firstColumn = 0;

    public int lastColumn = 0;
    //TODO Add Lastrow, Sheet number, Sheet name, first column number, last column number

    private static final List<String> DEFAULT_EXTS = ImmutableList.of("xlsx");

    public int getHeaderRow() {
      return headerRow;
    }

    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public List<String> getExtensions() {
      if (extensions == null) {
        return DEFAULT_EXTS;
      }
      return extensions;
    }

    @Override
    public int hashCode() {
      return 99;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      } else if (obj == null) {
        return false;
      } else if (getClass() == obj.getClass()) {
        return true;
      }
      return false;
    }
  }

}
