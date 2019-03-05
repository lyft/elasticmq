package org.elasticmq.actor.queue

import java.util.UUID

import scala.collection.mutable

import org.elasticmq.util.NowProvider
import org.elasticmq.{
  DeliveryReceipt,
  MessageAttribute,
  MessageData,
  MessageId,
  MessageStatistics,
  MillisNextDelivery,
  NeverReceived,
  NewMessageData,
  OnDateTimeReceived,
  QueueData,
  Received
}
import org.joda.time.DateTime

case class InternalMessage(id: String,
                           deliveryReceipts: mutable.Buffer[String],
                           var nextDelivery: Long,
                           content: String,
                           messageAttributes: Map[String, MessageAttribute],
                           created: DateTime,
                           var firstReceive: Received,
                           var receiveCount: Int,
                           isFifo: Boolean,
                           messageGroupId: Option[String],
                           messageDeduplicationId: Option[String])
    extends Comparable[InternalMessage] {

  // Priority queues have biggest elements first
  override def compareTo(other: InternalMessage): Int = {
    if (isFifo) {
      // FIFO messages should be ordered on when they were written to the queue
      -created.getMillis.compareTo(other.created.getMillis)
    } else {
      // Plain messages are technically not ordered, but AWS seems to offer a loose, non-guaranteed "next delivery"
      // ordering
      -nextDelivery.compareTo(other.nextDelivery)
    }
  }

  /**
    * Keep track of delivering this message to a client
    *
    * @param nextDeliveryMillis    When this message should become available for its next delivery
    */
  def trackDelivery(nextDeliveryMillis: MillisNextDelivery)(implicit nowProvider: NowProvider): Unit = {
    deliveryReceipts += DeliveryReceipt.generate(MessageId(id)).receipt
    nextDelivery = nextDeliveryMillis.millis
    receiveCount += 1

    if (firstReceive == NeverReceived) {
      firstReceive = OnDateTimeReceived(new DateTime(nowProvider.nowMillis))
    }
  }

  def toMessageData =
    MessageData(
      MessageId(id),
      deliveryReceipts.lastOption.map(DeliveryReceipt(_)),
      content,
      messageAttributes,
      MillisNextDelivery(nextDelivery),
      created,
      MessageStatistics(firstReceive, receiveCount),
      messageGroupId,
      messageDeduplicationId
    )

  def toNewMessageData =
    NewMessageData(Some(MessageId(id)),
                   content,
                   messageAttributes,
                   MillisNextDelivery(nextDelivery),
                   messageGroupId,
                   messageDeduplicationId)

  def deliverable(deliveryTime: Long): Boolean = nextDelivery <= deliveryTime
}

object InternalMessage {

  def from(newMessageData: NewMessageData, queueData: QueueData): InternalMessage = {
    val now = System.currentTimeMillis()
    new InternalMessage(
      newMessageData.id.getOrElse(generateId()).id,
      mutable.Buffer.empty,
      newMessageData.nextDelivery.toMillis(now, queueData.delay.getMillis).millis,
      newMessageData.content,
      newMessageData.messageAttributes,
      new DateTime(),
      NeverReceived,
      0,
      queueData.isFifo,
      newMessageData.messageGroupId,
      newMessageData.messageDeduplicationId
    )
  }

  private def generateId() = MessageId(UUID.randomUUID().toString)
}
