package iie.udps.common.hcatalog.scala

import org.apache.hadoop.mapreduce.{ OutputFormat, Job, JobContext, TaskAttemptContext, InputSplit, RecordWriter }
import org.apache.hadoop.io.NullWritable

import org.apache.hadoop.conf.Configuration
import org.apache.hive.hcatalog.mapreduce.{ HCatOutputFormat, HCatBaseOutputFormat }
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo
import org.apache.hive.hcatalog.data.HCatRecord
import org.apache.hive.hcatalog.data.schema.HCatSchema

import org.apache.spark.SerializableWritable

class SerHCatOutputFormat extends OutputFormat[NullWritable, SerializableWritable[HCatRecord]] {

  val instance = new HCatOutputFormat();

  def getRecordWriter(
    context: TaskAttemptContext) = {
    context.getConfiguration().set("mapred.task.id",
      context.getTaskAttemptID().toString())
    context.getConfiguration().setInt("mapred.task.partition",
      context.getTaskAttemptID().getTaskID().getId())
    val writer = instance
      .getRecordWriter(context)
    new RecordWriter[NullWritable, SerializableWritable[HCatRecord]]() {

      def write(key: NullWritable, value: SerializableWritable[HCatRecord]) {
        writer.write(key, value.value)
      }

      def close(context: TaskAttemptContext) {
        writer.close(context)
      }

    };
  }

  def checkOutputSpecs(context: JobContext) =
    instance.checkOutputSpecs(context)

  def getOutputCommitter(context: TaskAttemptContext) =
    instance.getOutputCommitter(context)

}

object SerHCatOutputFormat {
  def setOutput(outputJob: Job, outputJobInfo: OutputJobInfo) =
    HCatOutputFormat.setOutput(outputJob, outputJobInfo)

  def getTableSchema(conf: Configuration) =
    HCatBaseOutputFormat.getTableSchema(conf)

  def setSchema(outputJob: Job, schema: HCatSchema) =
    HCatOutputFormat.setSchema(outputJob, schema)

}