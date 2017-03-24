package com.github.jeanadrien.gatling.mqtt.actions

import akka.actor.ActorRef
import akka.pattern.AskTimeoutException
import akka.util.Timeout
import com.github.jeanadrien.gatling.mqtt.protocol.{ConnectionSettings, MqttComponents}
import io.gatling.commons.stats._
import io.gatling.commons.util.ClockSingleton._
import io.gatling.core.CoreComponents
import io.gatling.core.Predef._
import io.gatling.core.action.Action

import scala.util.{Failure, Success}
import scala.concurrent.duration._

/**
  *
  */
class ConnectAction(
    mqttComponents : MqttComponents,
    coreComponents : CoreComponents,
    connectionSettings : ConnectionSettings,
    connectionTimeout  : FiniteDuration,
    val next : Action
) extends MqttAction(mqttComponents, coreComponents) {

    import MessageListenerActor._
    import akka.pattern.ask
    import mqttComponents.system.dispatcher

    override val name = genName("mqttConnect")

    override def execute(session : Session) : Unit = recover(session) {
        mqttComponents.mqttEngine(session, connectionSettings).flatMap { mqtt =>
            val connectionId = genName("mqttConnection")

            val requestName = "connect"
            logger.debug(s"${connectionId}: Execute ${requestName}")

            val messageListener = mqttComponents.system
                .actorOf(MessageListenerActor.props(connectionId), "ml-" + connectionId)

            // connect
            val requestStartDate = nowMillis
            val connection = mqtt.callbackConnection()

            val listener = new ConnectionListener(connectionId, messageListener)
            connection.listener(listener)

            implicit val messageTimeout = Timeout(connectionTimeout)

            messageListener ? WaitForConnect onComplete { result =>
                logger.debug(s"${connectionId} : Connect onComplete ${result}")
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
                            Some("Wait for CONNECT timed out")
                        case Failure(t) =>
                            Some(t.getMessage)
                    }
                )

                if (result.isSuccess) {
                    next ! session.
                        set("connection", connection).
                        set("connectionId", connectionId).
                        set("listener", messageListener)
                }
                else {
                    next ! session.markAsFailed
                }
            }

            connection.connect(Callback.onSuccess[Void] { _ =>
                messageListener ! Connected
            } onFailure { th =>
                messageListener ! ConnectionFailed(th.getMessage)
            })
        }
    }

}
