package com.github.jeanadrien.gatling.mqtt.actions

import com.github.jeanadrien.gatling.mqtt.protocol.MqttComponents
import io.gatling.commons.stats._
import io.gatling.commons.util.ClockSingleton._
import io.gatling.core.CoreComponents
import io.gatling.core.action.Action
import io.gatling.core.session._
import org.fusesource.mqtt.client.{CallbackConnection, QoS, Topic}

/**
  *
  */
class DisconnectAction(
    mqttComponents : MqttComponents,
    coreComponents : CoreComponents,
    val next       : Action
) extends MqttAction(mqttComponents, coreComponents) {

    override val name = genName("mqttDisconnect")

    override def execute(session : Session) : Unit = recover(session)(for {
        connection <- session("connection").validate[CallbackConnection]
        connectionId <- session("connectionId").validate[String]
    } yield {
        val requestStartDate = nowMillis

        val requestName = "disconnect"

        logger.debug(s"${connectionId}: Execute ${requestName}")

        connection.disconnect(Callback.onSuccess { _ : Void =>
            val disconnectTimings = timings(requestStartDate)

            statsEngine.logResponse(
                session,
                requestName,
                disconnectTimings,
                OK,
                None,
                None
            )

            next ! session
        } onFailure { th =>
            val disconnectTimings = timings(requestStartDate)
            logger.warn(s"${connectionId}: Failed to DISCONNECT: ${th}")

            statsEngine.logResponse(
                session,
                requestName,
                disconnectTimings,
                KO,
                None,
                Some(th.getMessage)
            )

            next ! session
        })

    })

}
