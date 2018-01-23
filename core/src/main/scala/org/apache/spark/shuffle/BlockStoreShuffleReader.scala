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

package org.apache.spark.shuffle

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.storage.{BlockManager, ShuffleBlockFetcherIterator}
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
 */
/**
  * 该类用来通过shuffle请求其他节点的数据块存储,提取和读取partition范围内的分区
  * */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task */
  /**
    * 这里会解答两个问题:
    * 1)shuffle写存在Hash和sort两种方式,当读取时如何对应相应的读取方式.
    * 答案是:会根据不同的shuffle写入方式,采用相同的读取方式,然后写入到Hash列表中便于后继处理.
    * 在SparkEnv初始化过程中,会实例化ShuffleManager,BlockManager,MapOutputTracker等实例.其中ShuffleManager存在HashShuffleManager和
    * SortShuffleManager以及用户自定义的ShuffleManager.
    * 对于HashShuffleManager和SortShuffleManager在shuffle读取中均实例化BlockStoreShuffleReader,不同的是
    * HashShuffleManager持有FileShuffleBlockResolver和IndexShuffleBlockResolver
    *
    * 2)如何确定下游任务所读取数据的位置信息,位置信息包括所在节点,Executor编号和读取数据块序列.
    * 答案是:发送消息到Driver端的MapOutputTrackerMaster,可以获得上游shuffle的计算结果,通过MapStatus对象返回.
    *
    * shuffle read过程步骤:
    * 1在SparkEnv初始化过程中,会实例化ShuffleManager,BlockManager,MapOutputTracker等实例.其中ShuffleManager存在HashShuffleManager和
    * SortShuffleManager以及用户自定义的ShuffleManager.
    * 对于HashShuffleManager和SortShuffleManager在shuffle读取中均实例化BlockStoreShuffleReader,不同的是
    * HashShuffleManager持有FileShuffleBlockResolver和IndexShuffleBlockResolver
    *
    * 2 mapOutputTracker.getMapSizesByExecutorId的调用便是由Executor的MapOutputTracker发送获取结果状态的GetMapOutputStatus的消息
    * 发送消息到Driver端的MapOutputTrackerMaster,请求获取上游shuffle的输出结果的MapStatus.
    *
    * 3 获取这些shuffle结果位置后,会进行位置筛选,判断当前运行的数据是从本地读取还是远端拉取.
    * 如果是本地读取,直接BlockManager.getBlockData,在读取数据时会根据不同的写入方式采用不同的ShuffleBlockResolver(数据块解析器)进行读取;
    * 如果是远程读取,需要通过Netty网络的方式拉取数据并读取.在远程读取的过程中会采用多线程读取,一般来说会启动5个线程分别从5个节点上读取所有所需数据,
    * 每次请求的大小不会超过系统设置的1/5,该大小的设置参数是由spark.reducer.maxSizeInFlight进行配置.
    *
    * 4 读取数据后,判断shuffle依赖是不是定义聚合(Aggregation).如果需要则根据健值聚合.
    * 需要注意的是,如果上游ShuffleMapTask已经做了聚合,则在合并数据的基础上进行聚合.待数据处理完毕后,使用外部排序对数据进行排序并放入到存储中,
    * 至此完成shuffle读操作.
    *
    * Shuffle读对起点是由这个方法ShuffledRDD#compute发起.
    *
    * */
  override def read(): Iterator[Product2[K, C]] = {
    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue))

    // Wrap the streams for compression and encryption based on configuration
    val wrappedStreams = blockFetcherItr.map { case (blockId, inputStream) =>
      serializerManager.wrapStream(blockId, inputStream)
    }

    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { wrappedStream =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val readMetrics = context.taskMetrics.createTempShuffleReadMetrics()
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data. Note that if spark.shuffle.spill is disabled,
        // the ExternalSorter won't spill to disk.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }
  }
}
