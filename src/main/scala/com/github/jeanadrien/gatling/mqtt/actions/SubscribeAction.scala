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
class SubscribeAction(
    mqttComponents : MqttComponents,
    coreComponents : CoreComponents,
    topic : Expression[String],
    qos            : QoS,
    timeout        : FiniteDuration,
    val next       : Action
) extends MqttAction(mqttComponents, coreComponents) {

    import MessageListenerActor._
    import akka.pattern.ask
    import mqttComponents.system.dispatcher

    override val name = genName("mqttSubscribe")

    override def execute(session : Session) : Unit = recover(session)(for {
        listener <- session("listener").validate[ActorRef]
        connection <- session("connection").validate[CallbackConnection]
        connectionId <- session("connectionId").validate[String]
        resolvedTopic <- topic(session)
    } yield {
        implicit val messageTimeout = Timeout(timeout)

        val requestStartDate = nowMillis

        val requestName = "subscribe"

        logger.debug(s"${connectionId}: Execute ${requestName}:${resolvedTopic}")

        listener ? WaitForSubscribe onComplete { result =>
            logger.debug(s"${connectionId} : Subscribe onComplete ${result}")
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
                        Some("Wait for SUBSCRIBE timed out")
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

        val topics : Array[Topic] = Array(resolvedTopic).map((t : String) => new Topic(t, qos))
        connection.subscribe(topics, Callback.onSuccess { value : Array[Byte] =>
            listener ! Subscribed
        } onFailure { th =>
            listener ! SubscribeFailed(th.getMessage)
        })
    })

}
