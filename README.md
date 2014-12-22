# udps

## 概述

压缩包中主要包含了统一计算框架所需的封装库（udps-sdk-0.0.1.jar）以及其依赖的jar包汇编包（udps-depends-0.0.1.jar，包含了hadoop、hive、hcatalog、spark等组件）。
开发时同时使用上述两个包，而在平台上只需要udps-sdk，当然使用的第三方jar包，仍需要提交任务时上传。
另外，还包含了用于快速创建工程的工程模板，方面使用。

## 示例工程

提供了四种示例工程模板，在templates目录下，模板均为ecllipse工程，其中maven-java-project和mavene-scala-project支持使用maven进行编译，eclipse-scala-project和maven-scala-project需要提前安装scala ide插件。

### 用法

假设选择eclipse-java-project模板。

1. 将压缩包解压后的lib目录下的jar包复制到eclipse-java-project的lib目录（未存在，需要创建）下；
2. eclipse中选择Import->Existing Projects into Workspace，找到eclipse-java-project目录，选择导入。
