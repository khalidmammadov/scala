import java.text.SimpleDateFormat
import java.util.{Date, Timer, TimerTask}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

object Retry {

  def apply() = {
    val formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

    withRetry(3){
      Thread.sleep(2.seconds.toSeconds)
      println(s"Running ${formatter.format(new Date())}")
      throw new Throwable("bad")
    }
  }

  def withRetry[T](retryCount: Int, retryNo: Int = 0)(block: => T): T = {
    val fut = delayedRun(block, 2.seconds.toMillis).recoverWith{
      case e: Throwable =>
        if (retryCount > 0)
          Future(withRetry(retryCount-1, retryNo+1)(block))
        else
          throw e
    }
    Await.result(fut, 10.seconds)
  }

  def delayedRun[T](block: => T, delay: Long): Future[T] = {
    val p = Promise[T]
    val t = new Timer()

    val task = new TimerTask {
      override def run(): Unit = {
        t.cancel()
        p.complete(Try(block))
      }
    }

    t.schedule(task, delay)
    p.future
  }

}
