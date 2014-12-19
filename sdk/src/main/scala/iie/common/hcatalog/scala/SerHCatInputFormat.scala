package iie.common.hcatalog.scala

import org.apache.hadoop.mapreduce.{ InputFormat, Job, JobContext, TaskAttemptContext, InputSplit, RecordReader }
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.conf.Configuration
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat
import org.apache.hive.hcatalog.data.HCatRecord

import org.apache.spark.SerializableWritable

class SerHCatInputFormat extends InputFormat[NullWritable, SerializableWritable[HCatRecord]] {

  val instance = new HCatInputFormat();

  def getSplits(context: JobContext) =
    instance.getSplits(context)

  def createRecordReader(
    split: InputSplit, context: TaskAttemptContext) = {
    val reader = instance
      .createRecordReader(split, context);
    new RecordReader[NullWritable, SerializableWritable[HCatRecord]]() {
      def initialize(split: InputSplit, context: TaskAttemptContext) {
        reader.initialize(split, context);
      }

      def nextKeyValue() =
        reader.nextKeyValue()

      def getCurrentKey() =
        NullWritable.get()

      def getCurrentValue() =
        new SerializableWritable[HCatRecord](
          reader.getCurrentValue())

      def getProgress() =
        reader.getProgress()

      def close() =
        reader.close()

    }
  }
}

object SerHCatInputFormat {
  def setInput(conf: Configuration, dbName: String,
    tblName: String) =
    HCatInputFormat.setInput(conf, dbName, tblName)

  def setInput(job: Job, dbName: String, tblName: String) =
    HCatInputFormat.setInput(job, dbName, tblName)

  def setInput(job: Job, dbName: String, tblName: String, filter: String) =
    HCatInputFormat.setInput(job, dbName, tblName, filter);

  def getTableSchema(conf: Configuration) =
    HCatInputFormat.getDataColumns(conf)

}