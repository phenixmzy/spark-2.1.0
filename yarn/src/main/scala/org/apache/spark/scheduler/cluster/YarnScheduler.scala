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

import org.apache.hadoop.yarn.util.RackResolver
import org.apache.log4j.{Level, Logger}

import org.apache.spark._
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.util.Utils

/**
  * 该类继承自TaskSchedulerImpl,是一个TaskScheduler.
  * TaskScheduler是一个很重要的类,其为高层调度器DAGScheduler和SchedulerBackend之间的桥梁.
  * TaskScheduler负责具体任务的调度,而SchedulerBackend则负责application运行期间与底层的资源管理器(ResourceManager)进行交互.
  *
  * SchedulerBackend是一个trait,子类行会根据不同的运行模式分为本地运行模式,粗粒度运行模式(CoarseGrainedSchedulerBackend),细粒度Mesos运行模式(MesosSchedulerBackend).
  * 本地运行模式:LocalBackend
  * 粗粒度运行模式:分为standalone运行模式,Yarn运行模式,Mesos粗粒度运行模式
  *   -standalone运行模式:SparkDeploySchedulerBackend)
  *   -Yarn运行模式:对应于YarnSchedulerBackend,Yarn运行模式又包含:
  *     Yarn-client的YarnClientScheduler
  *     Yarn-cluster的YarnClusterSchedulerBackend
  *   -Mesos粗粒度运行模式:CoarseMesosSchedulerBackend
  * 细粒度运行模式:MesosSchedulerBackend
  *
  *
  *
  * */

private[spark] class YarnScheduler(sc: SparkContext) extends TaskSchedulerImpl(sc) {

  // RackResolver logs an INFO message whenever it resolves a rack, which is way too often.
  if (Logger.getLogger(classOf[RackResolver]).getLevel == null) {
    Logger.getLogger(classOf[RackResolver]).setLevel(Level.WARN)
  }

  // By default, rack is unknown
  override def getRackForHost(hostPort: String): Option[String] = {
    val host = Utils.parseHostPort(hostPort)._1
    Option(RackResolver.resolve(sc.hadoopConfiguration, host).getNetworkLocation)
  }
}
