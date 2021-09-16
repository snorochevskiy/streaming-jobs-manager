# Known problems

This document describes known problems that may periodically occur with managed spark applications.

## Missing block in HDFS

This NameNode/HDFS related error occurs from time to time, and the exact reason is not found.
```
Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2175)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2124)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2123)
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2123)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:990)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:990)
	at scala.Option.foreach(Option.scala:407)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:990)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2355)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2304)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2293)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:792)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2093)
	at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.writeWithV2(WriteToDataSourceV2Exec.scala:382)
	... 37 more
Caused by: org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-183115326-96.113.30.153-1605540097659:blk_1073969993_240365 file=/snmp-checkpoints/state/0/153/539.snapshot
	at org.apache.hadoop.hdfs.DFSInputStream.refetchLocations(DFSInputStream.java:879)
	at org.apache.hadoop.hdfs.DFSInputStream.chooseDataNode(DFSInputStream.java:862)
	at org.apache.hadoop.hdfs.DFSInputStream.chooseDataNode(DFSInputStream.java:841)
	at org.apache.hadoop.hdfs.DFSInputStream.blockSeekTo(DFSInputStream.java:567)
	at org.apache.hadoop.hdfs.DFSInputStream.readWithStrategy(DFSInputStream.java:757)
	at org.apache.hadoop.hdfs.DFSInputStream.read(DFSInputStream.java:829)
	at java.io.DataInputStream.read(DataInputStream.java:149)
	at net.jpountz.lz4.LZ4BlockInputStream.tryReadFully(LZ4BlockInputStream.java:269)
	at net.jpountz.lz4.LZ4BlockInputStream.refill(LZ4BlockInputStream.java:190)
	at net.jpountz.lz4.LZ4BlockInputStream.read(LZ4BlockInputStream.java:142)
	at java.io.DataInputStream.readInt(DataInputStream.java:387)
	at org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider.readSnapshotFile(HDFSBackedStateStoreProvider.scala:528)
	at org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider.$anonfun$loadMap$3(HDFSBackedStateStoreProvider.scala:376)
	at scala.Option.orElse(Option.scala:447)
	at org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider.$anonfun$loadMap$2(HDFSBackedStateStoreProvider.scala:376)
	at org.apache.spark.util.Utils$.timeTakenMs(Utils.scala:561)
	at org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider.loadMap(HDFSBackedStateStoreProvider.scala:356)
	at org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider.getStore(HDFSBackedStateStoreProvider.scala:204)
	at org.apache.spark.sql.execution.streaming.state.StateStore$.get(StateStore.scala:371)
	at org.apache.spark.sql.execution.streaming.state.StateStoreRDD.compute(StateStoreRDD.scala:90)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:349)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:313)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:349)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:313)
	at org.apache.spark.rdd.MapPartitionsRDD.compute(MapPartitionsRDD.scala:52)
	at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:349)
	at org.apache.spark.rdd.RDD.iterator(RDD.scala:313)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:90)
	at org.apache.spark.scheduler.Task.run(Task.scala:127)
	at org.apache.spark.executor.Executor$TaskRunner.$anonfun$run$3(Executor.scala:444)
	at org.apache.spark.util.Utils$.tryWithSafeFinally(Utils.scala:1377)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:447)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
	at java.lang.Thread.run(Thread.java:748)
```
The biggest problem with this issue is that it cannot be fixed by an auto-restart.
The solution is to remove checkpoints from HDFS and restart the job.
After the restart the job will pick up checkpoints from DynamoDB and continue working.

## Kinesis read rate limit exceeded

We are reading from same Kinesis stream from both DEV and PROD environments.
This may result in Kinesis read rate limit error if DEV and PROD jobs are reading from the same partition
at the same time.

TODO: add stackstack sample

This error is automatically fixed by auto-restart.

## Kafka timeout

Some kafka servers/partitions can become unavailable for ~15 minutes.
In this case SNMP sevone job crushes with following exception.
```
Driver stacktrace:
	at org.apache.spark.scheduler.DAGScheduler.failJobAndIndependentStages(DAGScheduler.scala:2175)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2(DAGScheduler.scala:2124)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$abortStage$2$adapted(DAGScheduler.scala:2123)
	at scala.collection.mutable.ResizableArray.foreach(ResizableArray.scala:62)
	at scala.collection.mutable.ResizableArray.foreach$(ResizableArray.scala:55)
	at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:2123)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1(DAGScheduler.scala:990)
	at org.apache.spark.scheduler.DAGScheduler.$anonfun$handleTaskSetFailed$1$adapted(DAGScheduler.scala:990)
	at scala.Option.foreach(Option.scala:407)
	at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:990)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:2355)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2304)
	at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:2293)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:49)
	at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:792)
	at org.apache.spark.SparkContext.runJob(SparkContext.scala:2093)
	at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.writeWithV2(WriteToDataSourceV2Exec.scala:382)
	... 37 more
Caused by: org.apache.kafka.common.errors.TimeoutException: Timeout of 60000ms expired before the position for partition sevone-spdb-28 could be determined
```
No extra activities required, auto-restart mechanism will handle this problem once Kafka is back to normal.

But if the Kafka is not healthy for 1 hour and more, then Kafka maintainers should be informed immediately.

## ExecutorLostFailure

Executor error:
```
ExecutorLostFailure (executor 29 exited caused by one of the running tasks)
Reason: Container from a bad node: container_1607965252297_0001_01_000034 on host: ip-96-113-30-150.nest.r53.xcal.tv.
Exit status: 143.
Diagnostics: [2020-12-15 15:17:18.802]Container killed on request. Exit code is 143
[2020-12-15 15:17:18.803]Container exited with a non-zero exit code 143. 
[2020-12-15 15:17:18.803]Killed by external signal
```

## All nodes marked as blacklisted

This is a rare and hard to detect problem: EMR is up, Spark application is running,
but since to executors available, data is not being processed.
For now, the solution is to recreate the whole cluster.