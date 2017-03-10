package com.github.jeanadrien.gatling.mqtt.actions

import com.softwaremill.quicklens._
import io.gatling.core.action.Action
import io.gatling.core.session._
import io.gatling.core.structure.ScenarioContext
import org.fusesource.mqtt.client.QoS
import scala.concurrent.duration._

/**
  *
  */
case class DisconnectActionBuilder(
    timeout : FiniteDuration = 30 seconds
) extends MqttActionBuilder {

    def timeout(duration : FiniteDuration) : DisconnectActionBuilder = this.modify(_.timeout).setTo(duration)

    override def build(
        ctx : ScenarioContext, next : Action
    ) : Action = {
        new DisconnectAction(
            mqttComponents(ctx),
            ctx.coreComponents,
            timeout,
            next
        )
    }

}

