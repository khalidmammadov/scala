import org.apache.spark.SparkFirehoseListener
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEvent}
import org.joda.time.LocalDateTime

import java.io.FileWriter


class AdHocListener extends SparkFirehoseListener {

  var appName: String = _

  def logApp(txt: String): Unit = {
    val fw = new FileWriter("/tmp/startedApps.log", true)
    fw.write(txt)
    fw.close()
  }

  override def onEvent(event: SparkListenerEvent): Unit = {
    try {
      val time = LocalDateTime.now()
      event match {
        case app: SparkListenerApplicationStart =>
          appName = app.appName
          logApp(s"${app.appName} started at $time \n")
          println(app.appName)
        case app: SparkListenerApplicationEnd =>
          logApp(s"$appName stopped at $time \n")
        case _ =>
      }
    } catch {
      case e: Throwable =>
        println(e)
    }
  }
}
