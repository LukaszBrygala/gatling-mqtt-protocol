package com.github.jeanadrien.gatling.mqtt.actions

import akka.actor.ActorRef
import akka.pattern.AskTimeoutException
import akka.util.Timeout
import com.github.jeanadrien.gatling.mqtt.protocol.MqttComponents
import io.gatling.commons.stats._
import io.gatling.commons.util.ClockSingleton._
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.session._
import org.fusesource.mqtt.client.{CallbackConnection, QoS, Topic}
import scala.util.{Failure, Success}
import scala.concurrent.duration._

/**
  *
  */
class DisconnectAction(
    mqttComponents : MqttComponents,
    coreComponents : CoreComponents,
    timeout  : FiniteDuration,
    val next       : Action
) extends MqttAction(mqttComponents, coreComponents) {

    import MessageListenerActor._
    import akka.pattern.ask
    import mqttComponents.system.dispatcher

    override val name = genName("mqttDisconnect")

    override def execute(session : Session) : Unit = recover(session)(for {
        listener <- session("listener").validate[ActorRef]
        connection <- session("connection").validate[CallbackConnection]
        connectionId <- session("connectionId").validate[String]
    } yield {
        implicit val messageTimeout = Timeout(timeout)

        val requestStartDate = nowMillis

        val requestName = "disconnect"

        logger.debug(s"${connectionId}: Execute ${requestName}")

        listener ? WaitForDisconnect onComplete { result =>
              logger.debug(s"${connectionId} : Disconnect onComplete ${result}")
              val latencyTimings = timings(requestStartDate)
        
              statsEngine.logResponse(
                  session,
                  requestName,
                  latencyTimings,
                  if (result.isSuccess) OK else KO,
                  None,
                  result match {
                      case Success(_) =>
                          None
                      case Failure(t) if t.isInstanceOf[AskTimeoutException] =>
                          Some("Wait for DISCONNECT timed out")
                      case Failure(t) =>
                          Some(t.getMessage)
                  }
              )

              if (result.isSuccess) {
                  next ! session
              }
              else {
                  next ! session.markAsFailed
              }
        }

        connection.connect(Callback.onSuccess[Void] { _ =>
            listener ! Disconnected
        } onFailure { th =>
            listener ! DisconnectFailed(th.getMessage)
        })
    })

}
