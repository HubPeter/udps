package iie.udps.api.spark;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * Spark转换算子基类。所有的Spark转换算子都应该继承该基类。
 * 
 * @author weixing
 *
 */
public abstract class TransformOp implements Serializable {
	private static final long serialVersionUID = 1444706869180116973L;

	/**
	 * 
	 * @param jsc
	 * @param conf
	 * @param rdds
	 * @return
	 */
	public abstract List<RDDWithSchema> transform(JavaSparkContext jsc,
			Configuration conf, List<RDDWithSchema> rdds);
}
