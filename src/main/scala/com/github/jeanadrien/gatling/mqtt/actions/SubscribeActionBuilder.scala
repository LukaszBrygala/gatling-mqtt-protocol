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
case class SubscribeActionBuilder(
    topic : Expression[String],
    qos   : QoS = QoS.AT_MOST_ONCE,
    timeout : FiniteDuration = 30 seconds
) extends MqttActionBuilder {

    def qos(newQos : QoS) : SubscribeActionBuilder = this.modify(_.qos).setTo(newQos)

    def qosAtMostOnce = qos(QoS.AT_MOST_ONCE)

    def qosAtLeastOnce = qos(QoS.AT_LEAST_ONCE)

    def qosExactlyOnce = qos(QoS.EXACTLY_ONCE)

    def timeout(duration : FiniteDuration) : SubscribeActionBuilder = this.modify(_.timeout).setTo(duration)

    override def build(
        ctx : ScenarioContext, next : Action
    ) : Action = {
        new SubscribeAction(
            mqttComponents(ctx),
            ctx.coreComponents,
            topic,
            qos,
            timeout,
            next
        )
    }

}

