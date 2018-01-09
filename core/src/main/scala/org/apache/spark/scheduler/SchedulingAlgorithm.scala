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
 * An interface for sort algorithm
 * FIFO: FIFO algorithm between TaskSetManagers
 * FS: FS algorithm between Pools, and FIFO or FS within Pools
 */
private[spark] trait SchedulingAlgorithm {
  def comparator(s1: Schedulable, s2: Schedulable): Boolean
}

/**
  * 根调度池rootPool直接包含了任务集管理器TaskSetManager,在比较时,先比较作业的优先级(根据作业的编号判断,编号越小优先级越高);
  * 如果是同一个作业,则比较调度阶段(根据调度阶段的编号判断,编号越小优先级越高)
  * */
private[spark] class FIFOSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    //获取调度的优先级,实际上是作业的编号
    val priority1 = s1.priority
    val priority2 = s2.priority
    var res = math.signum(priority1 - priority2)
    if (res == 0) {//同一个作业,则比较调度阶段
      val stageId1 = s1.stageId
      val stageId2 = s2.stageId
      res = math.signum(stageId1 - stageId2)
    }
    res < 0
  }
}

/**
  * FAIR调度策略包含两层调度,第一层根据根调度rootPool包含下级调度池,第二层为下级调度池包含了多个TaskSetManager.
  * 在fairscheduler.xml文件中,包含了多个下级调度池Pool配置选项,其中minShare是最小任务数,weight任务的权重,minShare和weight用来设置第一级调度算法,
  * schedulingMode用来设计二级调度池.
  * 先获取两个调度的饥饿程度,饥饿程度为正在运行的任务是否为小于最小的任务,如果是则表示该调度阶段正处于饥饿程度.
  * 获取饥饿程度后进行如下比较:
  * -如果某个调度处于饥饿状态,另一个处于非饥饿状态,则先满足处于饥饿状态的调度;
  * -如果两个调度都处于饥饿状态,则比较资源比,先满足资源比小的调度;
  * -如果两个调度都处于非饥饿状态,则比较权重,先满足权重小的调度;
  * -以上情况均相同,则根据调度名称排序.
  * */
private[spark] class FairSchedulingAlgorithm extends SchedulingAlgorithm {
  override def comparator(s1: Schedulable, s2: Schedulable): Boolean = {
    //最小任务数
    val minShare1 = s1.minShare
    val minShare2 = s2.minShare
    //正在运行任务数
    val runningTasks1 = s1.runningTasks
    val runningTasks2 = s2.runningTasks
    //饥饿程序,判断正在运行的任务数是否小于最小任务数
    val s1Needy = runningTasks1 < minShare1
    val s2Needy = runningTasks2 < minShare2
    //资源比,正在运行的任务数／最小任务数
    val minShareRatio1 = runningTasks1.toDouble / math.max(minShare1, 1.0)
    val minShareRatio2 = runningTasks2.toDouble / math.max(minShare2, 1.0)
    //权重比,正在运行的任务数／任务的权重
    val taskToWeightRatio1 = runningTasks1.toDouble / s1.weight.toDouble
    val taskToWeightRatio2 = runningTasks2.toDouble / s2.weight.toDouble
    //执行判断
    var compare = 0
    if (s1Needy && !s2Needy) {
      return true
    } else if (!s1Needy && s2Needy) {
      return false
    } else if (s1Needy && s2Needy) {
      compare = minShareRatio1.compareTo(minShareRatio2)
    } else {
      compare = taskToWeightRatio1.compareTo(taskToWeightRatio2)
    }
    if (compare < 0) {
      true
    } else if (compare > 0) {
      false
    } else {
      s1.name < s2.name
    }
  }
}

