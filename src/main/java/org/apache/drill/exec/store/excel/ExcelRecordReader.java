package org.apache.drill.exec.store.excel;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;


public class ExcelRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExcelRecordReader.class);

  private static final int MAX_RECORDS_PER_BATCH = 8096;

  private String inputPath;

  private BufferedReader reader;

  private DrillBuf buffer;

  private VectorContainerWriter writer;

  private ExcelFormatPlugin.ExcelFormatConfig config;

  private int lineCount;

  private XSSFWorkbook workbook;

  private FSDataInputStream fsStream;

  private XSSFSheet sheet;

  public static final String SAFE_WILDCARD = "_$";

  public static final String SAFE_SEPARATOR = "_";

  public static final String PARSER_WILDCARD = ".*";

  private String[] excelFieldNames;

  private Iterator<Row> rowIterator;

  private int totalColumnCount;

  public ExcelRecordReader(FragmentContext fragmentContext, String inputPath, DrillFileSystem fileSystem, List<SchemaPath> columns, ExcelFormatPlugin.ExcelFormatConfig config) throws OutOfMemoryException {
    try {
      //TODO Get sheet name or ID from input path
      this.fsStream = fileSystem.open(new Path(inputPath));
      this.inputPath = inputPath;
      this.lineCount = 0;
      this.reader = new BufferedReader(new InputStreamReader(fsStream.getWrappedStream(), "UTF-8"));
      this.config = config;
      this.buffer = fragmentContext.getManagedBuffer();
      setColumns(columns);

    } catch (IOException e) {
      logger.debug("Excel Reader Plugin: " + e.getMessage());
    }
  }

  public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
    this.writer = new VectorContainerWriter(output);
    try {
      //Open the file
      //TODO Get sheet name or index from the file path
      this.workbook = new XSSFWorkbook(this.fsStream.getWrappedStream());

      //Evaluate formulae
      FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();
      this.sheet = workbook.getSheetAt(0);
      this.workbook.setMissingCellPolicy(Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);

      //Get the field names
      int columnCount = sheet.getRow(0).getPhysicalNumberOfCells();
      this.excelFieldNames = new String[columnCount];
      this.rowIterator = sheet.iterator();

      if (rowIterator.hasNext()) {
        while (this.lineCount < config.headerRow) {
          Row row = rowIterator.next();
          this.lineCount++;
          System.out.println("Skipping row: " + this.lineCount);
        }
        Row row = rowIterator.next();
        this.totalColumnCount = row.getLastCellNum();

        Iterator<Cell> cellIterator = row.cellIterator();
        int colPosition = 0;
        while (cellIterator.hasNext()) {
          Cell cell = cellIterator.next();
          CellValue cellValue = evaluator.evaluate(cell);
          switch (cellValue.getCellTypeEnum()) {
            case STRING:
              this.excelFieldNames[colPosition] = cell.getStringCellValue().replaceAll("_", "__").replace(PARSER_WILDCARD, SAFE_WILDCARD).replaceAll("\\.", SAFE_SEPARATOR);
              break;
            case NUMERIC:
              this.excelFieldNames[colPosition] = String.valueOf(cell.getNumericCellValue());
              break;
          }
          colPosition++;
        }
      }


    } catch (java.io.IOException e) {
      throw UserException.dataReadError(e).build(logger);
    }
  }

  public int next() {
    this.writer.allocate();
    this.writer.reset();

    int skipRows = config.headerRow;
    int recordCount = 0;
    int sheetCount = workbook.getNumberOfSheets();
    int lastRow = config.lastRow;


    BaseWriter.MapWriter map = this.writer.rootAsMap();
    int colPosition = 0;
    FormulaEvaluator evaluator = workbook.getCreationHelper().createFormulaEvaluator();

    try {
      while (recordCount > lastRow && rowIterator.hasNext()) {
        lineCount++;
        if (recordCount > 0) {
          this.writer.setPosition(recordCount);
          map.start();
        }

        Row row = rowIterator.next();
        Iterator<Cell> cellIterator = row.cellIterator();

        colPosition = 0;
        String fieldName;
        if (row.getLastCellNum() < totalColumnCount) {
          System.out.println("Wrong number of columns in row.");
        }

        for (int cn = 0; cn < totalColumnCount; cn++) {

          Cell cell = row.getCell(cn);

          CellValue cellValue = evaluator.evaluate(cell);
          //TODO Strip newlines from field names.
          fieldName = excelFieldNames[colPosition];

          if (cellValue == null) {
            String fieldValue = "";
            byte[] bytes = fieldValue.getBytes("UTF-8");
            this.buffer.setBytes(0, bytes, 0, bytes.length);
            map.varChar(fieldName).writeVarChar(0, bytes.length, buffer);
          } else {
            switch (cellValue.getCellTypeEnum()) {
              case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                  map.timeStamp(fieldName).writeTimeStamp(cell.getDateCellValue().getTime());
                } else {
                  double fieldNumValue = cell.getNumericCellValue();
                  map.float8(fieldName).writeFloat8(fieldNumValue);
                }
                break;
              case STRING:
                String fieldValue = "";
                fieldValue = cellValue.formatAsString();
                if (fieldValue.length() > 1) {
                  fieldValue = fieldValue.substring(1);
                  fieldValue = fieldValue.substring(0, fieldValue.length() - 1);
                }

                byte[] bytes = fieldValue.getBytes("UTF-8");
                this.buffer.setBytes(0, bytes, 0, bytes.length);
                map.varChar(fieldName).writeVarChar(0, bytes.length, buffer);
                break;
              case FORMULA:
                //Not again
                break;
              case BLANK:
                System.out.println("Blank Cell");
                break;
            }
          }
          colPosition++;
        }
        map.end();
        recordCount++;
      }


      this.writer.setValueCount(recordCount);
      return recordCount;

    } catch (final Exception e) {
      throw UserException.dataReadError(e).build(logger);
    }
  }

  public void close() throws Exception {
    this.reader.close();
  }

}
