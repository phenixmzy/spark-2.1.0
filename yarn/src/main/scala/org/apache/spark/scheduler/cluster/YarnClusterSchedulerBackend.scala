/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler.cluster

import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn.{ApplicationMaster, YarnSparkHadoopUtil}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

/**
  * 1 客户端提交应用程序时,启动Yarn.Client向Yarn中提交应用程序,包括AM的命令,提交给AM的程序和需要在Executor中运行的程序等.
  * 2 RM收到请求后,在集群中选择一个NM,为该应用程序分派第一个用于启动运行AM的container.在该AM上运行SparkContext等并进行初始化.
  * 3 AM向RM注册,用户可以直接通过RM查询application的运行状态,然后它将以轮询的方式申请和获取申请资源,并监控任务的运行状态直到运行结束.
  * 4 一旦AM申请到资源,便会与NodeManager通信,要求它在获得的container中启动CoarseGrainedExecutorBackend,
  * CoarseGrainedExecutorBackend启动后会向AM的SparkContext注册并申请TaskSet
  * 5 AM的SparkContext分配任务集给CoarseGrainedExecutorBackend执行,CoarseGrainedExecutorBackend运行任务并向AM汇报运行的状态和进度,
  * 让AM随时掌握各task运行状态,从而可以在任务失败时重新启动
  * 6 application运行完成后,客户端的SparkContext向RM申请注销并关闭自身
  * */

private[spark] class YarnClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends YarnSchedulerBackend(scheduler, sc) {

  override def start() {
    val attemptId = ApplicationMaster.getAttemptId
    bindToYarn(attemptId.getApplicationId(), Some(attemptId))
    super.start()
    totalExpectedExecutors = YarnSparkHadoopUtil.getInitialTargetExecutorNumber(sc.conf)
  }

  override def getDriverLogUrls: Option[Map[String, String]] = {
    var driverLogs: Option[Map[String, String]] = None
    try {
      val yarnConf = new YarnConfiguration(sc.hadoopConfiguration)
      val containerId = YarnSparkHadoopUtil.get.getContainerId

      val httpAddress = System.getenv(Environment.NM_HOST.name()) +
        ":" + System.getenv(Environment.NM_HTTP_PORT.name())
      // lookup appropriate http scheme for container log urls
      val yarnHttpPolicy = yarnConf.get(
        YarnConfiguration.YARN_HTTP_POLICY_KEY,
        YarnConfiguration.YARN_HTTP_POLICY_DEFAULT
      )
      val user = Utils.getCurrentUserName()
      val httpScheme = if (yarnHttpPolicy == "HTTPS_ONLY") "https://" else "http://"
      val baseUrl = s"$httpScheme$httpAddress/node/containerlogs/$containerId/$user"
      logDebug(s"Base URL for logs: $baseUrl")
      driverLogs = Some(Map(
        "stdout" -> s"$baseUrl/stdout?start=-4096",
        "stderr" -> s"$baseUrl/stderr?start=-4096"))
    } catch {
      case e: Exception =>
        logInfo("Error while building AM log links, so AM" +
          " logs link will not appear in application UI", e)
    }
    driverLogs
  }
}
