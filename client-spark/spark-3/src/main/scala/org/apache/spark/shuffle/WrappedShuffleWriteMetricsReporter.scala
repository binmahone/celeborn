package org.apache.spark.shuffle

import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleWriteMetricsReporter}

class WrappedShuffleWriteMetricsReporter(
    wrapped: SQLShuffleWriteMetricsReporter)
  extends ShuffleWriteMetricsReporter {

  // get private field metrics from metricsReporter
  import scala.reflect.runtime.universe._
  import scala.reflect.runtime.currentMirror
  private val mirror = currentMirror.reflect(wrapped)
  private val privateFieldSymbol = mirror.symbol.typeSignature.member(TermName("metrics")).asTerm
  private val metrics = mirror.reflectField(privateFieldSymbol).get.
    asInstanceOf[Map[String, SQLMetric]]

  // these constants are defined in org.apache.spark.sql.rapids.SQLShuffleWriteMetricsReporter
  // of Rapids plugin
  private[this] val _compressTime =
    metrics("shuffleWriteCompressTime")
  private[this] val _congestionTime =
    metrics("shuffleWriteCongestionTime")
  private[this] val _closeTime =
    metrics("shuffleWriteCloseTime")

  // TODO double check if need synchronized ???

  def incCloseTime(v: Long): Unit = {
    _closeTime.add(v)
  }

  def incCompressTime(v: Long): Unit = {
    _compressTime.add(v)
  }

  def incCongestionTime(v: Long): Unit = {
    _congestionTime.add(v)
  }

  override private[spark] def incBytesWritten(v: Long): Unit = wrapped.incBytesWritten(v)

  override private[spark] def incRecordsWritten(v: Long): Unit = wrapped.incRecordsWritten(v)

  override private[spark] def incWriteTime(v: Long): Unit = wrapped.incWriteTime(v)

  override private[spark] def decBytesWritten(v: Long): Unit = wrapped.decBytesWritten(v)

  override private[spark] def decRecordsWritten(v: Long): Unit = wrapped.decRecordsWritten(v)
}
