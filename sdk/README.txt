安装udps-sdk

    配置:Yarn:YARN 应用程序类路径 +

        /opt/udps/lib/*
        /opt/udps/libext/*
        $CDH_HCAT_HOME/share/hcatalog/*
        $CDH_HIVE_HOME/lib/*
        /etc/hive/conf
        /opt/cloudera/parcels/CDH/lib/spark/lib/spark-assembly-1.6.0-cdh5.8.0-hadoop2.6.0-cdh5.8.0.jar

    配置:Yarn:hadoop-env.sh 的 Gateway 客户端环境高级配置代码段（安全阈） +

        HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$(echo /opt/cloudera/parcels/CDH/lib/hive-hcatalog/share/hcatalog/*.jar /opt/cloudera/parcels/CDH/lib/hive/lib/*.jar /opt/udps/lib/*.jar /opt/udps/libext/*.jar):/etc/hive/conf:/opt/cloudera/parcels/CDH/lib/spark/lib/spark-assembly-1.6.0-cdh5.8.0-hadoop2.6.0-cdh5.8.0.jar

    部署sdk包:

        clush -g all "mkdir -p /opt/udps/{lib,libext}"
        clush -g all --copy udps-sdk-0.3-spark1.6.0-cdh5.8.0.jar --dest /opt/udps/lib/
