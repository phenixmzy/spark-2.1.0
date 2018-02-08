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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.yarn.api.records.YarnApplicationState

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.deploy.yarn.{Client, ClientArguments, YarnSparkHadoopUtil}
import org.apache.spark.internal.Logging
import org.apache.spark.launcher.SparkAppHandle
import org.apache.spark.scheduler.TaskSchedulerImpl
/**
  * yarn-client的工作流程:
  * 1 启动应用程序,在SparkConetxt启动过程中,初始化DAGScheduler和TaskSchedulerImpl调度器,使用反射方法初始化YarnScheduler和
  * YarnClientSchedulerBackend.YarnClientSchedulerBackend在内部启动终端点DriverEndpoint和Client,
  * 然后Client向Yarn集群的RM申请启动AM.
  * 2 RM收到请求后,在集群中选择一个NM,为该应用程序分派第一个用于启动运行AM的container.与yarn-cluster不同的是,
  * 在该AM上不运行在SparkContext,只与SparkContext联系进行资源分派.
  * 3 客户端中的SparkContext启动完毕后,YarnSchedulerEndpoint与AM建立通信,向RM注册,根据任务信息向RM申请container资源.
  * 4 一旦AM申请到资源,便会与NodeManager通信,要求它在获得的container中启动CoarseGrainedExecutorBackend,
  * CoarseGrainedExecutorBackend启动后会向客户端中的SparkContext注册并申请TaskSet.
  * 5 客户端中的SparkContext分配任务集给CoarseGrainedExecutorBackend执行,CoarseGrainedExecutorBackend运行任务并向
  * 终端点DriverEndpoint汇报运行的状态和进度,让客户端随时掌握各task运行状态,从而可以在任务失败时重新启动.
  * 6 application运行完成后,客户端的SparkContext向RM申请注销并关闭自身.
  *
  * */
private[spark] class YarnClientSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext)
  extends YarnSchedulerBackend(scheduler, sc)
  with Logging {

  private var client: Client = null
  private var monitorThread: MonitorThread = null

  /**
   * Create a Yarn client to submit an application to the ResourceManager.
   * This waits until the application is running.
   */
  override def start() {
    val driverHost = conf.get("spark.driver.host")
    val driverPort = conf.get("spark.driver.port")
    val hostport = driverHost + ":" + driverPort
    sc.ui.foreach { ui => conf.set("spark.driver.appUIAddress", ui.appUIAddress) }

    val argsArrayBuf = new ArrayBuffer[String]()
    argsArrayBuf += ("--arg", hostport)

    logDebug("ClientArguments called with: " + argsArrayBuf.mkString(" "))
    val args = new ClientArguments(argsArrayBuf.toArray)
    totalExpectedExecutors = YarnSparkHadoopUtil.getInitialTargetExecutorNumber(conf)
    //启动client
    client = new Client(args, conf)
    bindToYarn(client.submitApplication(), None)

    // SPARK-8687: Ensure all necessary properties have already been set before
    // we initialize our driver scheduler backend, which serves these properties
    // to the executors
    super.start()
    waitForApplication()

    // SPARK-8851: In yarn-client mode, the AM still does the credentials refresh. The driver
    // reads the credentials from HDFS, just like the executors and updates its own credentials
    // cache.
    if (conf.contains("spark.yarn.credentials.file")) {
      YarnSparkHadoopUtil.get.startCredentialUpdater(conf)
    }
    monitorThread = asyncMonitorApplication()
    monitorThread.start()
  }

  /**
   * Report the state of the application until it is running.
   * If the application has finished, failed or been killed in the process, throw an exception.
   * This assumes both `client` and `appId` have already been set.
   */
  private def waitForApplication(): Unit = {
    assert(client != null && appId.isDefined, "Application has not been submitted yet!")
    val (state, _) = client.monitorApplication(appId.get, returnOnRunning = true) // blocking
    if (state == YarnApplicationState.FINISHED ||
      state == YarnApplicationState.FAILED ||
      state == YarnApplicationState.KILLED) {
      throw new SparkException("Yarn application has already ended! " +
        "It might have been killed or unable to launch application master.")
    }
    if (state == YarnApplicationState.RUNNING) {
      logInfo(s"Application ${appId.get} has started running.")
    }
  }

  /**
   * We create this class for SPARK-9519. Basically when we interrupt the monitor thread it's
   * because the SparkContext is being shut down(sc.stop() called by user code), but if
   * monitorApplication return, it means the Yarn application finished before sc.stop() was called,
   * which means we should call sc.stop() here, and we don't allow the monitor to be interrupted
   * before SparkContext stops successfully.
   */
  private class MonitorThread extends Thread {
    private var allowInterrupt = true

    override def run() {
      try {
        val (state, _) = client.monitorApplication(appId.get, logApplicationReport = false)
        logError(s"Yarn application has already exited with state $state!")
        allowInterrupt = false
        sc.stop()
      } catch {
        case e: InterruptedException => logInfo("Interrupting monitor thread")
      }
    }

    def stopMonitor(): Unit = {
      if (allowInterrupt) {
        this.interrupt()
      }
    }
  }

  /**
   * Monitor the application state in a separate thread.
   * If the application has exited for any reason, stop the SparkContext.
   * This assumes both `client` and `appId` have already been set.
   */
  private def asyncMonitorApplication(): MonitorThread = {
    assert(client != null && appId.isDefined, "Application has not been submitted yet!")
    val t = new MonitorThread
    t.setName("Yarn application state monitor")
    t.setDaemon(true)
    t
  }

  /**
   * Stop the scheduler. This assumes `start()` has already been called.
   */
  override def stop() {
    assert(client != null, "Attempted to stop this scheduler before starting it!")
    if (monitorThread != null) {
      monitorThread.stopMonitor()
    }

    // Report a final state to the launcher if one is connected. This is needed since in client
    // mode this backend doesn't let the app monitor loop run to completion, so it does not report
    // the final state itself.
    //
    // Note: there's not enough information at this point to provide a better final state,
    // so assume the application was successful.
    client.reportLauncherState(SparkAppHandle.State.FINISHED)

    super.stop()
    YarnSparkHadoopUtil.get.stopCredentialUpdater()
    client.stop()
    logInfo("Stopped")
  }

}
