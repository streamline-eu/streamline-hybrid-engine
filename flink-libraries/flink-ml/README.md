Flink-ML constitutes the machine learning library of Apache Flink.
Our vision is to make machine learning easily accessible to a wide audience and yet to achieve extraordinary performance.
For this purpose, Flink-ML is based on two pillars:

Flink-ML contains implementations of popular ML algorithms which are highly optimized for Apache Flink.
Theses implementations allow to scale to data sizes which vastly exceed the memory of a single computer.
Flink-ML currently comprises the following algorithms:

* Classification
  * Soft-margin SVM
  * Passice-aggressive classifier
* Regression
  * Multiple linear regression
* Recommendation
  * Alternating least squares (ALS)
  * Matrix Factorization
  * Factorization Machine
* Sketch
  * Tug of War
  * Bloomfilter
  * Minhash

Since most of the work in data analytics is related to post- and pre-processing of data where the performance is not crucial, Flink wants to offer a simple abstraction to do that.
Linear algebra, as common ground of many ML algorithms, represents such a high-level abstraction.
Therefore, Flink will support the Mahout DSL as a execution engine and provide tools to neatly integrate the optimized algorithms into a linear algebra program.

Flink-ML also provides a parameter server architecture, which can serve as a framework for distributed, online machine learning algorithms. This implementation is based on the Stream API. 

Parameter server is an abstraction for model-parallel machine learning (see the work of [Li et al.](https://doi.org/10.1145/2640087.2644155)).
Our implementation could be used with the Streaming API:
it can take a `DataStream` of data-points as input, and produce a `DataStream` of model updates. This way, we can implement both online and offline ML algorithms. Currently only asynchronous training is supported.


### API

We can use the Parameter Server in the following way:

![Parameter Server architecture](PS_figures.png)

Basically, we can access the Parameter Server by defining a [```WorkerLogic```](https://github.com/gaborhermann/flink-ps/blob/master/src/main/scala/hu/sztaki/ilab/ps/WorkerLogic.scala), which can *pull* or *push* parameters. We provide input data to the worker via a Flink ```DataStream```.

We need to implement the ```WorkerLogic``` trait
```scala
trait WorkerLogic[T, P, WorkerOut] extends Serializable {
  def onRecv(data: T, ps: ParameterServerClient[P, WorkerOut]): Unit
  def onPullRecv(paramId: Int, paramValue: P, ps: ParameterServerClient[P, WorkerOut]): Unit
}
```
where we can handle incoming data (`onRecv`), *pull* parameters from the Parameter Server, handle the answers to the pulls (`onPullRecv`), and *push* parameters to the Parameter Server or *output* results. We can use the ```ParameterServerClient```:
```scala
trait ParameterServerClient[P, WorkerOut] extends Serializable {
  def pull(id: Int): Unit
  def push(id: Int, deltaUpdate: P): Unit
  def output(out: WorkerOut): Unit
}
```

When we defined our worker logic we can wire it into a Flink job with the `transform` method of [```FlinkParameterServer```](src/main/scala/hu/sztaki/ilab/ps/FlinkParameterServer.scala).

```scala
def transform[T, P, WorkerOut](
  trainingData: DataStream[T],
  workerLogic: WorkerLogic[T, P, WorkerOut],
  paramInit: => Int => P,
  paramUpdate: => (P, P) => P,
  workerParallelism: Int,
  psParallelism: Int,
  iterationWaitTime: Long): DataStream[Either[WorkerOut, (Int, P)]]
```

Besides the `trainingData` stream and the `workerLogic`, we need to define how the Parameter Server should initialize a parameter based on the parameter id (`paramInit`), and how to update a parameter based on a received push (`paramUpdate`). We must also define how many parallel instances of workers and parameter servers we should use (`workerParallelism` and `psParallelism`), and the `iterationWaitTime` (see [Limitations](README.md#limitations)).

### Limitations

We implement the two-way communication of workers and the parameter server with Flink Streaming [iterations](https://ci.apache.org/projects/flink/flink-docs-release-1.3/dev/datastream_api.html#iterations), which is not yet production-ready. The main issues are
- **Sometimes deadlocks due to cyclic backpressure.** A workaround could be to limiting the amount of unanswered pulls per worker (e.g. by using [WorkerLogic.addPullLimiter](src/main/scala/hu/sztaki/ilab/ps/WorkerLogic.scala#L169)), or manually limiting the input rate of data on the input stream. In any case, deadlock would still be possible.
- **Termination is not defined for finite input.** As a workaround, we can set the `iterationwaitTime` for the milliseconds to wait before shutting down if there's no messages sent along the iteration (see the Flink (Java Docs)https://ci.apache.org/projects/flink/flink-docs-master/api/java/)).
- **No fault tolerance.**

All these issues are being addressed in [FLIP-15](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=66853132) and [FLIP-16](https://cwiki.apache.org/confluence/display/FLINK/FLIP-16%3A+Loop+Fault+Tolerance) and soon to be fixed. Until then, we need to use workarounds.



Flink-ML has just been recently started.
As part of Apache Flink, it heavily relies on the active work and contributions of its community and others.
Thus, if you want to add a new algorithm to the library, then find out [how to contribute]((http://flink.apache.org/how-to-contribute.html)) and open a pull request!

