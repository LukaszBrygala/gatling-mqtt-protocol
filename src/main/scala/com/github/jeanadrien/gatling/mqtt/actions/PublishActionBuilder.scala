package com.github.jeanadrien.gatling.mqtt.actions

import com.softwaremill.quicklens._
import io.gatling.commons.validation._
import io.gatling.core.action.Action
import io.gatling.core.session._
import io.gatling.core.structure.ScenarioContext
import org.fusesource.mqtt.client.QoS

/**
  *
  */
case class PublishActionBuilder(
    topic : Expression[String],
    payload : Expression[Array[Byte]],
    qos : QoS = QoS.AT_MOST_ONCE,
    retain : Boolean = false,
    requestName : Expression[String] = _ => Success("publish")
) extends MqttActionBuilder {

    def qos(newQos : QoS) : PublishActionBuilder = this.modify(_.qos).setTo(newQos)

    def qosAtMostOnce = qos(QoS.AT_MOST_ONCE)

    def qosAtLeastOnce = qos(QoS.AT_LEAST_ONCE)

    def qosExactlyOnce = qos(QoS.EXACTLY_ONCE)

    def retain(newRetain : Boolean) : PublishActionBuilder = this.modify(_.retain).setTo(newRetain)

    def requestName(newName : Expression[String]) : PublishActionBuilder = this.modify(_.requestName).setTo(newName)

    override def build(
        ctx : ScenarioContext, next : Action
    ) : Action = {
        new PublishAction(
            mqttComponents(ctx),
            ctx.coreComponents,
            topic,
            payload,
            qos,
            retain,
            requestName,
            next
        )
    }

}
