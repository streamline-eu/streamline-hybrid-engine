package com.twalthr

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

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{CsvInputFormat, RowCsvInputFormat}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.TableException
import org.apache.flink.table.sources._
import org.apache.flink.types.Row

import scala.collection.mutable

/**
  * A [[org.apache.flink.table.sources.BatchTableSource]] and [[org.apache.flink.table.sources.StreamTableSource]]
  * for simple CSV files with a (logically) unlimited number of fields.
  *
  * @param path The path to the CSV file.
  * @param fieldNames The names of the table fields.
  * @param fieldTypes The types of the table fields.
  * @param fieldDelim The field delimiter, "," by default.
  * @param rowDelim The row delimiter, "\n" by default.
  * @param quoteCharacter An optional quote character for String values, null by default.
  * @param ignoreFirstLine Flag to ignore the first line, false by default.
  * @param ignoreComments An optional prefix to indicate comments, null by default.
  * @param lenient Flag to skip records with parse error instead to fail, false by default.
  */
class CustomCsvTableSource(
    private val path: String,
    private val fieldNames: Array[String],
    private val fieldTypes: Array[TypeInformation[_]],
    private val fieldDelim: String = CsvInputFormat.DEFAULT_FIELD_DELIMITER,
    private val rowDelim: String = CsvInputFormat.DEFAULT_LINE_DELIMITER,
    private val quoteCharacter: Character = null,
    private val ignoreFirstLine: Boolean = false,
    private val ignoreComments: String = null,
    private val lenient: Boolean = false)
  extends BatchTableSource[Row]
  with DefinedProctimeAttribute
  with DefinedRowtimeAttribute
  with StreamTableSource[Row]
  with ProjectableTableSource[Row] {

  /**
    * A [[BatchTableSource]] and [[StreamTableSource]] for simple CSV files with a
    * (logically) unlimited number of fields.
    *
    * @param path The path to the CSV file.
    * @param fieldNames The names of the table fields.
    * @param fieldTypes The types of the table fields.
    */
  def this(path: String, fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]) =
    this(path, fieldNames, fieldTypes, CsvInputFormat.DEFAULT_FIELD_DELIMITER,
      CsvInputFormat.DEFAULT_LINE_DELIMITER, null, false, null, false)

  if (fieldNames.length != fieldTypes.length) {
    throw TableException("Number of field names and field types must be equal.")
  }

  private val returnType = new RowTypeInfo(fieldTypes, fieldNames)

  private var selectedFields: Array[Int] = fieldTypes.indices.toArray

  /**
    * Returns the data of the table as a [[DataSet]] of [[Row]].
    *
    * NOTE: This method is for internal use only for defining a [[org.apache.flink.table.sources.TableSource]].
    *       Do not use it in Table API programs.
    */
  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    execEnv.createInput(createCsvInput(), returnType)
  }

  /** Returns the [[RowTypeInfo]] for the return type of the [[CustomCsvTableSource]]. */
  override def getReturnType: RowTypeInfo = returnType

  private var outOfOrder: Long = -1L

  def setOutOfOrder(amount: Long, unit: TimeUnit): Unit = {
    if (amount < 0) {
      this.outOfOrder = -1L
    } else {
      this.outOfOrder = unit.toMillis(amount)
    }
  }

  /**
    * Returns the data of the table as a [[DataStream]] of [[Row]].
    *
    * NOTE: This method is for internal use only for defining a [[org.apache.flink.table.sources.TableSource]].
    *       Do not use it in Table API programs.
    */
  override def getDataStream(streamExecEnv: StreamExecutionEnvironment): DataStream[Row] = {
    val input = streamExecEnv.createInput(createCsvInput(), returnType)
    if (outOfOrder != -1) {
      input.assignTimestampsAndWatermarks(new WatermarkAssigner(outOfOrder))
    } else {
      input // ending sources emit a final Long.MAX_VALUE watermark
    }
  }

  /** Returns a copy of [[org.apache.flink.table.sources.TableSource]] with ability to project fields */
  override def projectFields(fields: Array[Int]): CustomCsvTableSource = {

    val filteredFields = fields.filter(_ < fieldNames.length)

    val newFieldNames: Array[String] = filteredFields.map(fieldNames(_))
    val newFieldTypes: Array[TypeInformation[_]] = filteredFields.map(fieldTypes(_))

    val source = new CustomCsvTableSource(path,
      newFieldNames,
      newFieldTypes,
      fieldDelim,
      rowDelim,
      quoteCharacter,
      ignoreFirstLine,
      ignoreComments,
      lenient)
    source.setOutOfOrder(outOfOrder, TimeUnit.MILLISECONDS)
    source.setTimePrefix(timePrefix)
    source.setRowtime(rowtime)
    source.selectedFields = filteredFields
    source
  }

  private def createCsvInput(): RowCsvInputFormat = {
    val inputFormat = new RowCsvInputFormat(
      new Path(path),
      fieldTypes,
      rowDelim,
      fieldDelim,
      selectedFields)

    inputFormat.setSkipFirstLineAsHeader(ignoreFirstLine)
    inputFormat.setLenient(lenient)
    if (quoteCharacter != null) {
      inputFormat.enableQuotedStringParsing(quoteCharacter)
    }
    if (ignoreComments != null) {
      inputFormat.setCommentPrefix(ignoreComments)
    }

    inputFormat
  }

  override def equals(other: Any): Boolean = other match {
    case that: CustomCsvTableSource => returnType == that.returnType &&
        path == that.path &&
        fieldDelim == that.fieldDelim &&
        rowDelim == that.rowDelim &&
        quoteCharacter == that.quoteCharacter &&
        ignoreFirstLine == that.ignoreFirstLine &&
        ignoreComments == that.ignoreComments &&
        lenient == that.lenient
    case _ => false
  }

  override def hashCode(): Int = {
    returnType.hashCode()
  }

  private var timePrefix = ""

  def setTimePrefix(prefix: String): Unit = {
    timePrefix = prefix
  }

  override def getProctimeAttribute: String = timePrefix + "proctime"

  var rowtime = false

  def setRowtime(rt: Boolean): Unit = {
    rowtime = rt
  }

  override def getRowtimeAttribute: String = if (rowtime) {
    timePrefix + "rowtime"
  } else {
    null
  }
}

object CustomCsvTableSource {

  /**
    * A builder for creating [[CustomCsvTableSource]] instances.
    *
    * For example:
    *
    * {{{
    *   val source: CsvTableSource = new CsvTableSource.builder()
    *     .path("/path/to/your/file.csv")
    *     .field("myfield", Types.STRING)
    *     .field("myfield2", Types.INT)
    *     .build()
    * }}}
    *
    */
  class Builder {

    private val schema: mutable.LinkedHashMap[String, TypeInformation[_]] =
      mutable.LinkedHashMap[String, TypeInformation[_]]()
    private var quoteCharacter: Character = _
    private var path: String = _
    private var fieldDelim: String = CsvInputFormat.DEFAULT_FIELD_DELIMITER
    private var lineDelim: String = CsvInputFormat.DEFAULT_LINE_DELIMITER
    private var isIgnoreFirstLine: Boolean = false
    private var commentPrefix: String = _
    private var lenient: Boolean = false

    /**
      * Sets the path to the CSV file. Required.
      *
      * @param path the path to the CSV file
      */
    def path(path: String): Builder = {
      this.path = path
      this
    }

    /**
      * Sets the field delimiter, "," by default.
      *
      * @param delim the field delimiter
      */
    def fieldDelimiter(delim: String): Builder = {
      this.fieldDelim = delim
      this
    }

    /**
      * Sets the line delimiter, "\n" by default.
      *
      * @param delim the line delimiter
      */
    def lineDelimiter(delim: String): Builder = {
      this.lineDelim = delim
      this
    }

    /**
      * Adds a field with the field name and the type information. Required.
      * This method can be called multiple times. The call order of this method defines
      * also the order of thee fields in a row.
      *
      * @param fieldName the field name
      * @param fieldType the type information of the field
      */
    def field(fieldName: String, fieldType: TypeInformation[_]): Builder = {
      if (schema.contains(fieldName)) {
        throw new IllegalArgumentException(s"Duplicate field name $fieldName.")
      }
      schema += (fieldName -> fieldType)
      this
    }

    /**
      * Sets a quote character for String values, null by default.
      *
      * @param quote the quote character
      */
    def quoteCharacter(quote: Character): Builder = {
      this.quoteCharacter = quote
      this
    }

    /**
      * Sets a prefix to indicate comments, null by default.
      *
      * @param prefix the prefix to indicate comments
      */
    def commentPrefix(prefix: String): Builder = {
      this.commentPrefix = prefix
      this
    }

    /**
      * Ignore the first line. Not skip the first line by default.
      */
    def ignoreFirstLine(): Builder = {
      this.isIgnoreFirstLine = true
      this
    }

    /**
      * Skip records with parse error instead to fail. Throw an exception by default.
      */
    def ignoreParseErrors(): Builder = {
      this.lenient = true
      this
    }

    /**
      * Apply the current values and constructs a newly-created [[CustomCsvTableSource]].
      *
      * @return a newly-created [[CustomCsvTableSource]].
      */
    def build(): CustomCsvTableSource = {
      if (path == null) {
        throw new IllegalArgumentException("Path must be defined.")
      }
      if (schema.isEmpty) {
        throw new IllegalArgumentException("Fields can not be empty.")
      }
      new CustomCsvTableSource(
        path,
        schema.keys.toArray,
        schema.values.toArray,
        fieldDelim,
        lineDelim,
        quoteCharacter,
        isIgnoreFirstLine,
        commentPrefix,
        lenient)
    }

  }

  /**
    * Return a new builder that builds a [[CustomCsvTableSource]].
    *
    * For example:
    *
    * {{{
    *   val source: CsvTableSource = CsvTableSource
    *     .builder()
    *     .path("/path/to/your/file.csv")
    *     .field("myfield", Types.STRING)
    *     .field("myfield2", Types.INT)
    *     .build()
    * }}}
    *
    * @return a new builder to build a [[CustomCsvTableSource]]
    */
  def builder(): Builder = new Builder
}

class WatermarkAssigner(val outOfOrder: Long) extends AssignerWithPunctuatedWatermarks[Row] {

  var lastWatermark = Long.MinValue

  override def extractTimestamp(t: Row, prevTs: Long): Long = t.getField(0).asInstanceOf[Long]

  override def checkAndGetNextWatermark(t: Row, extractedTs: Long): Watermark = {
    val nextWatermark = extractedTs - outOfOrder
    if (lastWatermark < nextWatermark) {
      lastWatermark = nextWatermark
      new Watermark(lastWatermark)
    } else {
      null
    }
  }
}
