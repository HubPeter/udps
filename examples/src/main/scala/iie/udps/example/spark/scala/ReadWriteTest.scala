package iie.hadoop.spark.scala

import iie.common.hcatalog.scala.{ SerHCatInputFormat, SerHCatOutputFormat }
import org.apache.hadoop.io.NullWritable
import org.apache.hive.hcatalog.data.HCatRecord
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo
import org.apache.spark.rdd.{ PairRDDFunctions, RDD }
import org.apache.spark.{ SerializableWritable, SparkConf, SparkContext };

/**
 * Created by weixing on 2014/12/8.
 */
object ReadWriteTest {
  def main(args: Array[String]) {
    val dbName = "wx";
    val inputTbl = "tbl_spark_in";
    val outputTbl = "tbl_spark_out";

    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("ReadWriteTest"))

    val inputJob = Job.getInstance()
    SerHCatInputFormat.setInput(inputJob.getConfiguration, dbName, inputTbl)

    val dataset = sc.newAPIHadoopRDD(inputJob.getConfiguration, classOf[SerHCatInputFormat], classOf[NullWritable], classOf[SerializableWritable[HCatRecord]])

    val outputJob = Job.getInstance();
    outputJob.setOutputFormatClass(classOf[SerHCatOutputFormat])
    outputJob.setOutputKeyClass(classOf[NullWritable])
    outputJob.setOutputValueClass(classOf[SerializableWritable[HCatRecord]])
    SerHCatOutputFormat.setOutput(outputJob, OutputJobInfo.create(dbName, outputTbl, null))
    SerHCatOutputFormat.setSchema(outputJob, SerHCatOutputFormat.getTableSchema(outputJob.getConfiguration))
    new PairRDDFunctions[NullWritable, SerializableWritable[HCatRecord]](dataset).saveAsNewAPIHadoopDataset(outputJob.getConfiguration)
    sc.stop()
    sys.exit()
  }
}
