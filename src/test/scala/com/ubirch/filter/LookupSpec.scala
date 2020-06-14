package com.ubirch.filter

import java.util.{Base64, UUID}

import com.github.nosan.embedded.cassandra.cql.CqlScript
import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{Config, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging
import com.ubirch.filter.model.{FilterError, FilterErrorDeserializer, Rejection, RejectionDeserializer, Values}
import com.ubirch.filter.model.eventlog.CassandraFinder
import com.ubirch.filter.services.kafka.{AbstractFilterService, DefaultFilterService, FakeFilterReplayAttack}
import com.ubirch.filter.ConfPaths.{ConsumerConfPaths, ProducerConfPaths}
import com.ubirch.filter.cache.{Cache, CacheMockAlwaysFalse}
import com.ubirch.filter.services.config.ConfigProvider
import com.ubirch.filter.util.Messages
import com.ubirch.kafka.MessageEnvelope
import com.ubirch.protocol.ProtocolMessage
import com.ubirch.util.PortGiver
import io.prometheus.client.CollectorRegistry
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{Deserializer, Serializer}
import org.json4s.JsonAST.{JObject, JString}

class LookupSpec extends TestBase with EmbeddedCassandra with LazyLogging {

  implicit val seMsgEnv: Serializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeSerializer
  implicit val deMsgEnv: Deserializer[MessageEnvelope] = com.ubirch.kafka.EnvelopeDeserializer
  implicit val deRej: Deserializer[Rejection] = RejectionDeserializer
  implicit val deError: Deserializer[FilterError] = FilterErrorDeserializer

  override protected def beforeAll(): Unit = {
    cassandra.start()
    cassandra.executeScripts(
      CqlScript.statements(
        "CREATE KEYSPACE IF NOT EXISTS event_log WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
        "USE event_log;",
        "drop table if exists events;",
        """
          |create table if not exists events (
          |    id text,
          |    customer_id text,
          |    service_class text,
          |    category text,
          |    signature text,
          |    event text,
          |    year int ,
          |    month int,
          |    day int,
          |    hour int,
          |    minute int,
          |    second int,
          |    milli int,
          |    event_time timestamp,
          |    nonce text,
          |    PRIMARY KEY ((id, category), year, month, day, hour)
          |) WITH CLUSTERING ORDER BY (year desc, month DESC, day DESC);
        """.stripMargin,
        "drop table if exists lookups;",
        """
          |create table if not exists lookups (
          |    key text,
          |    value text,
          |    name text,
          |    category text,
          |    PRIMARY KEY ((value, category), name)
          |);
        """.stripMargin
      )
    )
  }

  override protected def afterAll(): Unit = {
    cassandra.stop()
  }

  override protected def beforeEach(): Unit = {
    CollectorRegistry.defaultRegistry.clear()
    cassandra.executeScripts(CqlScript.statements("TRUNCATE events;", "TRUNCATE lookups;"))
    Thread.sleep(5000)
  }

  val payload = "c29tZSBieXRlcyEAAQIDnw=="


  val insertEventSql: String =
    s"""
       |INSERT INTO events (id, customer_id, service_class, category, event, event_time, year, month, day, hour, minute, second, milli, signature, nonce)
       | VALUES ('$payload', 'customer_id', 'service_class', '${Values.UPP_CATEGORY}', '{
       |   "hint":0,
       |   "payload":"$payload",
       |   "signature":"5aTelLQBerVT/vJiL2qjZCxWxqlfwT/BaID0zUVy7LyUC9nUdb02//aCiZ7xH1HglDqZ0Qqb7GyzF4jtBxfSBg==",
       |   "signed":"lRKwjni1ymWXEeiBhcg+pwAOTQCwc29tZSBieXRlcyEAAQIDnw==",
       |   "uuid":"8e78b5ca-6597-11e8-8185-c83ea7000e4d",
       |   "version":34
       |}', '2019-01-29T17:00:28.333Z', 2019, 5, 2, 19, 439, 16, 0, '0681D35827B17104A2AACCE5A08C4CD1BC8A5EF5EFF4A471D15976693CC0D6D67392F1CACAE63565D6E521D2325A998CDE00A2FEF5B65D0707F4158000EB6D05',
       |'34376336396166392D336533382D343665652D393063332D616265313364383335353266');
    """.stripMargin

  val insertLookupSql: String =
    s"""
       |INSERT INTO lookups (name, category, key, value) VALUES ('signature', '${Values.UPP_CATEGORY}', 'c29tZSBieXRlcyEAAQIDnw==', '5aTelLQBerVT/vJiL2qjZCxWxqlfwT/BaID0zUVy7LyUC9nUdb02//aCiZ7xH1HglDqZ0Qqb7GyzF4jtBxfSBg==');
    """.stripMargin

  "Lookup Spec" must {

    "consume and process successfully when Found" in {

      cassandra.executeScripts(
        CqlScript.statements(
          insertEventSql
        )
      )

      implicit val kafkaConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig(kafkaPort = PortGiver.giveMeKafkaPort, zooKeeperPort = PortGiver.giveMeZookeeperPort)

      val bootstrapServers = "localhost:" + kafkaConfig.kafkaPort

      def testInjector(bootstrapServers: String, consumerTopic: String): InjectorHelper = new InjectorHelper(List(new Binder {
        override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(bootstrapServers, consumerTopic))
        override def Cache: ScopedBindingBuilder = bind(classOf[Cache]).to(classOf[CacheMockAlwaysFalse])
      })) {}

      EmbeddedKafka.start()

      val msgEnvelope = generateMessageEnvelope(payload)
      publishToKafka(Messages.jsonTopic, msgEnvelope)
      Thread.sleep(1000)
      val Injector = testInjector(bootstrapServers, Messages.jsonTopic)
      val consumer = Injector.get[DefaultFilterService]
      consumer.consumption.startPolling()
      Thread.sleep(1000)

      val rejection = consumeFirstMessageFrom[Rejection](Messages.rejectionTopic)
      rejection.message mustBe Messages.foundInCacheMsg
      rejection.rejectionName mustBe Messages.replayAttackName

      EmbeddedKafka.stop()

    }
  }

  /**
    * Overwrite default bootstrap server and topic values of the kafka consumer and producers
    */
  def customTestConfigProvider(bootstrapServers: String, consumerTopic: String): ConfigProvider = new ConfigProvider {
    override def conf: Config = super.conf.withValue(
      ConsumerConfPaths.BOOTSTRAP_SERVERS,
      ConfigValueFactory.fromAnyRef(bootstrapServers)
    ).withValue(
      ProducerConfPaths.BOOTSTRAP_SERVERS,
      ConfigValueFactory.fromAnyRef(bootstrapServers)
    ).withValue(
      ConsumerConfPaths.TOPICS,
      ConfigValueFactory.fromAnyRef(consumerTopic)
    )
  }

  /**
    * Method to generate a message envelope as expected by the filter service.
    *
    * @param payload Payload is a random value if not defined explicitly.
    * @return
    */
  private def generateMessageEnvelope(payload: Object = Base64.getEncoder.encode(UUID.randomUUID().toString.getBytes())): MessageEnvelope = {

    val pm = new ProtocolMessage(1, UUID.randomUUID(), 0, payload)
    pm.setSignature(org.bouncycastle.util.Strings.toByteArray("1111"))
    val ctxt = JObject("customerId" -> JString(UUID.randomUUID().toString))
    MessageEnvelope(pm, ctxt)
  }
}
