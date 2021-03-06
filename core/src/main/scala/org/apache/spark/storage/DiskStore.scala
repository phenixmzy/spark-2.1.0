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

package org.apache.spark.storage

import java.io.{FileOutputStream, IOException, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import com.google.common.io.Closeables

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Stores BlockManager blocks on disk.
 */
/**
  * 存储BlockManager数据块到磁盘中.
  * 其主要提供put/get方法对数据块进行读取和写入操作.
  * 磁盘数据存储在spark.local.dir目录中,而块数据存放在该目录对二级目录下,存放目录有BlockManager来决定,具体是BlockManager＃getFile.
  * 一级目录是spark-UUID.randomUUID的16位uuid,二级目录最多不超过64个目录,以数据命名,文件名以block.name命名.
  * */
private[spark] class DiskStore(conf: SparkConf, diskManager: DiskBlockManager) extends Logging {

  private val minMemoryMapBytes = conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")

  def getSize(blockId: BlockId): Long = {
    diskManager.getFile(blockId.name).length
  }

  /**
   * Invokes the provided callback function to write the specific block.
   *
   * @throws IllegalStateException if the block already exists in the disk store.
   */
  def put(blockId: BlockId)(writeFunc: FileOutputStream => Unit): Unit = {
    if (contains(blockId)) {
      throw new IllegalStateException(s"Block $blockId is already present in the disk store")
    }
    logDebug(s"Attempting to put block $blockId")
    val startTime = System.currentTimeMillis
    // 获取需要写入文件句柄
    val file = diskManager.getFile(blockId)
    val fileOutputStream = new FileOutputStream(file)
    var threwException: Boolean = true
    try {
      // 使用回调方法,写入前需要把值类型数据序列化成数据流
      writeFunc(fileOutputStream)
      threwException = false
    } finally {
      try {
        Closeables.close(fileOutputStream, threwException)
      } finally {
         if (threwException) {
          remove(blockId)
        }
      }
    }
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName,
      Utils.bytesToString(file.length()),
      finishTime - startTime))
  }

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    put(blockId) { fileOutputStream =>
      val channel = fileOutputStream.getChannel
      Utils.tryWithSafeFinally {
        bytes.writeFully(channel)
      } {
        channel.close()
      }
    }
  }

  def getBytes(blockId: BlockId): ChunkedByteBuffer = {
    val file = diskManager.getFile(blockId.name)
    val channel = new RandomAccessFile(file, "r").getChannel
    Utils.tryWithSafeFinally {
      // For small files, directly read rather than memory map
      // 小文件则直接读取到buf中返回ChunkedByteBuffer;大文件则用内存映射再返回ChunkedByteBuffer对象
      if (file.length < minMemoryMapBytes) {
        val buf = ByteBuffer.allocate(file.length.toInt)
        channel.position(0)
        while (buf.remaining() != 0) {
          if (channel.read(buf) == -1) {
            throw new IOException("Reached EOF before filling buffer\n" +
              s"offset=0\nfile=${file.getAbsolutePath}\nbuf.remaining=${buf.remaining}")
          }
        }
        buf.flip()
        new ChunkedByteBuffer(buf)
      } else {
        //将指定通道的文件区域映射到内存中
        new ChunkedByteBuffer(channel.map(MapMode.READ_ONLY, 0, file.length))
      }
    } {
      channel.close()
    }
  }

  def remove(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    if (file.exists()) {
      val ret = file.delete()
      if (!ret) {
        logWarning(s"Error deleting ${file.getPath()}")
      }
      ret
    } else {
      false
    }
  }

  def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    file.exists()
  }
}
