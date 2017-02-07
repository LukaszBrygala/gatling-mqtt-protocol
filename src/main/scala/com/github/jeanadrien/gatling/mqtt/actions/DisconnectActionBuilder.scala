package com.github.jeanadrien.gatling.mqtt.actions

import com.softwaremill.quicklens._
import io.gatling.core.action.Action
import io.gatling.core.session._
import io.gatling.core.structure.ScenarioContext
import org.fusesource.mqtt.client.QoS

/**
  *
  */
case class DisconnectActionBuilder(
) extends MqttActionBuilder {

    override def build(
        ctx : ScenarioContext, next : Action
    ) : Action = {
        new DisconnectAction(
            mqttComponents(ctx),
            ctx.coreComponents,
            next
        )
    }

}

