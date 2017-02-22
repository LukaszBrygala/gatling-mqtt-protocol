package com.github.jeanadrien.gatling.mqtt.actions

import akka.actor.ActorRef
import akka.pattern.AskTimeoutException
import akka.util.Timeout
import com.github.jeanadrien.gatling.mqtt.protocol.MqttComponents
import io.gatling.commons.stats.{KO, OK}
import io.gatling.commons.util.ClockSingleton._
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.session._
import org.fusesource.mqtt.client.{CallbackConnection, QoS}

import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}

/**
  *
  */
class PublishAndWaitAction(
    mqttComponents : MqttComponents,
    coreComponents : CoreComponents,
    publishTopic  : Expression[String],
    receiveTopic  : Expression[String],
    payload       : Expression[Array[Byte]],
    payloadFeedback : Array[Byte] => Array[Byte] => Boolean,
    qos           : QoS,
    retain        : Boolean,
    timeout       : FiniteDuration,
    requestName   : Expression[String],
    val next      : Action
) extends MqttAction(mqttComponents, coreComponents) {

    import MessageListenerActor._
    import akka.pattern.ask
    import mqttComponents.system.dispatcher

    override val name = genName("mqttPublishAndWait")

    override def execute(session : Session) : Unit = recover(session)(for {
        connection <- session("connection").validate[CallbackConnection]
        listener <- session("listener").validate[ActorRef]
        connectionId <- session("connectionId").validate[String]
        resolvedPublishTopic <- publishTopic(session)
        resolvedReceiveTopic <- receiveTopic(session)
        resolvedPayload <- payload(session)
        resolvedRequestName <- requestName(session)
    } yield {
        implicit val messageTimeout = Timeout(timeout)

        val requestStartDate = nowMillis

        logger.debug(s"${connectionId} : Execute ${resolvedRequestName} Payload: ${resolvedPayload} Publish: ${resolvedPublishTopic} Receive: ${resolvedReceiveTopic}")

        val payloadCheck = payloadFeedback(resolvedPayload)

        listener ? WaitForMessage(resolvedReceiveTopic, payloadCheck) onComplete { result =>
            val latencyTimings = timings(requestStartDate)

            statsEngine.logResponse(
                session,
                resolvedRequestName,
                latencyTimings,
                if (result.isSuccess) OK else KO,
                None,
                result match {
                    case Success(_) =>
                        logger.trace(s"${connectionId}: Wait for PUBLISH succeded")
                        None
                    case Failure(t) if t.isInstanceOf[AskTimeoutException] =>
                        logger.warn(s"${connectionId}: Wait for PUBLISH back from mqtt timed out on ${resolvedReceiveTopic}")
                        Some("Wait for PUBLISH timed out")
                    case Failure(t) =>
                        logger
                            .warn(s"${connectionId}: Failed to receive PUBLISH back from mqtt on ${resolvedReceiveTopic}: ${t}")
                        Some(t.getMessage)
                }
            )

            if (result.isFailure) {
                listener ! CancelWaitForMessage(resolvedReceiveTopic, payloadCheck)
                next ! session.markAsFailed
            }
            else {
                next ! session
            }
        }

        connection.publish(resolvedPublishTopic, resolvedPayload, qos, retain, Callback.onSuccess[Void] { _ =>
            // nop
        } onFailure { th =>
            logger.warn(s"${connectionId}: Failed to publish on ${resolvedPublishTopic}: ${th}")
            statsEngine.reportUnbuildableRequest(session, "publish", th.getMessage)
        })
    })
}
