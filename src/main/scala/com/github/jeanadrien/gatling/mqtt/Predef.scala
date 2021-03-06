package com.github.jeanadrien.gatling.mqtt

import java.nio.charset.StandardCharsets

import com.github.jeanadrien.gatling.mqtt.actions._
import com.github.jeanadrien.gatling.mqtt.protocol.MqttProtocolBuilder
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session._

/**
  *
  */
object Predef {

    def mqtt(implicit configuration : GatlingConfiguration) = MqttProtocolBuilder(configuration)

    def connect = ConnectActionBuilder()

    def disconnect = DisconnectActionBuilder()

    def subscribe(topic : Expression[String]) = SubscribeActionBuilder(topic)

    def publish(topic : Expression[String], payload : Expression[Array[Byte]]) = PublishActionBuilder(topic, payload)

    def publishAndWait(
        publishTopic : Expression[String], receiveTopic : Expression[String], payload : Expression[Array[Byte]]
    ) = PublishAndWaitActionBuilder(publishTopic, receiveTopic, payload)

    def publishAndMeasure(
        topic : Expression[String], payload : Expression[Array[Byte]]
    ) = PublishAndMeasureActionBuilder(topic, payload)

    def waitForMessages = WaitForMessagesActionBuilder

    implicit def expressionOfStringToByteArray(in : Expression[String]) : Expression[Array[Byte]] =
        in.map(_.getBytes(StandardCharsets.UTF_8))
}
