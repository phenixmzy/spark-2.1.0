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

package org.apache.spark.scheduler

/**
 * A backend interface for scheduling systems that allows plugging in different ones under
 * TaskSchedulerImpl. We assume a Mesos-like model where the application gets resource offers as
 * machines become available and can launch tasks on them.
 */
/**
  * 调度系统的后台接口,它允许插入不同的TaskSchedulerImpl.我们假定了一个类似于Mesos的模型它能为application提供资源获取机制,运行task.
  * 位于TaskScheduler下层，用于对接不同的资源管理系统，SchedulerBackend是个接口，需要实现的主要方法如下.
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
  * */
private[spark] trait SchedulerBackend {
  private val appId = "spark-application-" + System.currentTimeMillis

  def start(): Unit
  def stop(): Unit
  /**
    * reviveOffers是一个重要方法：SchedulerBackend把自己手头上的可用资源交给TaskScheduler，TaskScheduler根据调度策略分配给排队的任务吗，返回一批可执行的任务描述，
    * SchedulerBackend负责launchTask，即最终把task塞到了executor模型上，executor里的线程池会执行task的run()
    * */
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  def killTask(taskId: Long, executorId: String, interruptThread: Boolean): Unit =
    throw new UnsupportedOperationException
  def isReady(): Boolean = true

  /**
   * Get an application ID associated with the job.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Get the attempt ID for this run, if the cluster manager supports multiple
   * attempts. Applications run in client mode will not have attempt IDs.
   *
   * @return The application attempt id, if available.
   */
  def applicationAttemptId(): Option[String] = None

  /**
   * Get the URLs for the driver logs. These URLs are used to display the links in the UI
   * Executors tab for the driver.
   * @return Map containing the log names and their respective URLs
   */
  def getDriverLogUrls: Option[Map[String, String]] = None

}
