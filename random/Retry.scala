import java.text.SimpleDateFormat
import java.util.concurrent.{Executors, TimeUnit}
import java.util.{Date, Timer, TimerTask}
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

object Retry {
  val t = new Timer()
  val formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

  //  private val threadPool = Executors.newScheduledThreadPool(9) // You can configure the thread pool size as needed

  //    implicit val ctx = scala.concurrent.ExecutionContext.Implicits.global
  //  implicit val ctx = new ExecutionContext {
  //    private val threadPool = Executors.newFixedThreadPool(8) // You can configure the thread pool size as needed
  //
  //    override def execute(runnable: Runnable): Unit = {
  //      threadPool.submit(runnable)
  //    }
  //
  //    override def reportFailure(cause: Throwable): Unit = {
  //      // Handle or log the failure here
  //      println(s"An error occurred: $cause")
  //    }
  //  }


  def apply() = {

    (1 to 50).map { i =>
      var count = 0
      println(s"Starting Future at Thread ${Thread.currentThread().getId}")
      withRetry(3) {
        //          println(s"Starting to sleep for $i ${formatter.format(new Date())} attempt $count in Thread ${Thread.currentThread().getId}")
        println(s"Running code $i ${formatter.format(new Date())} attempt $count in Thread ${Thread.currentThread().getId}")
        count += 1
        //          if (count < 2)
        throw new Throwable(s"bad $i")

        111
      }
    }.map{
      Await.ready(_, 60.seconds)
    }.foreach { f =>
      f.onComplete {
        case Failure(e) => println(s"Exception: $e")
        case Success(v) => println(s"Value :$v")
      }
    }

    t.cancel()
  }

  def withRetry[T](retryCount: Int, retryNo: Int = 0)(block: => T): Future[T] = {
    val fut = delayedRun(block, 2.seconds.toMillis).recoverWith{
      case e: Throwable =>
        if (retryCount > 0) {
          println(s"Retrying with new Future ${formatter.format(new Date())} in Thread ${Thread.currentThread().getId}")
          withRetry(retryCount-1, retryNo+1)(block)
        }
        else
          throw e
    }
    fut
  }

  def delayedRun[T](block: => T, delay: Long): Future[T] = {
    val p = Promise[T]

    val task = new TimerTask {
      override def run(): Unit = {
        println(s"Timer Thread ${Thread.currentThread().getId}")
        Future{
          println(s"Running time task in new Future ${formatter.format(new Date())} in Thread ${Thread.currentThread().getId}")
          p.complete(Try(block))
        }
      }
    }

    t.schedule(task, delay)
    p.future
  }

  //  def delayedRun2[T](block: => T, delay: Long): Future[T] = {
  //    val p = Promise[T]
  //
  //    val task = new Runnable {
  //      override def run(): Unit = {
  //        println(s"Runnable Thread ${Thread.currentThread().getId}")
  //        //        printThreadSize()
  //          println(s"Running time task in new Future ${formatter.format(new Date())} in Thread ${Thread.currentThread().getId}")
  //          p.complete(Try(block))
  //      }
  //    }
  //
  //    threadPool.schedule(task, delay, TimeUnit.MILLISECONDS)
  //    p.future
  //  }
}
