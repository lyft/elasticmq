package org.elasticmq.actor

import akka.actor.{ActorRef, Props}
import org.elasticmq.actor.queue.QueueActor
import org.elasticmq.actor.reply._
import org.elasticmq.msg.{CreateQueue, DeleteQueue, _}
import org.elasticmq.util.{Logging, NowProvider}
import org.elasticmq.{QueueAlreadyExists, QueueData, QueueDoesNotExist}

import scala.reflect._

class QueueManagerActor(nowProvider: NowProvider) extends ReplyingActor with Logging {
  type M[X] = QueueManagerMsg[X]
  val ev = classTag[M[Unit]]

  private val queues = collection.mutable.HashMap[String, ActorRef]()

  def receiveAndReply[T](msg: QueueManagerMsg[T]): ReplyAction[T] = msg match {
    case CreateQueue(queueData) => {
      if (queues.contains(queueData.name)) {
        logger.debug(s"Cannot create queue, as it already exists: $queueData")
//        Left(new QueueAlreadyExists(queueData.name))

        //TODO flag for unsafe mode
        logger.info(s"Creating queue $queueData")
        Right(queues.get(queueData.name).getOrElse(createQueueActor(nowProvider, queueData)))
      } else {
        logger.info(s"Creating queue $queueData")
        val actor = createQueueActor(nowProvider, queueData)
        queues(queueData.name) = actor
        Right(actor)
      }
    }

    case DeleteQueue(queueName) => {
      logger.info(s"Deleting queue $queueName")
      queues.remove(queueName).foreach(context.stop(_))
    }

    case LookupQueue(queueName) =>
      var result = queues.get(queueName)
      logger.debug(s"Looking up queue $queueName, found?: ${result.isDefined}")

      // TODO flag for unsafe mode
      if (!result.isDefined) {
        val queueData = QueueData(
          queueName,
          MillisVisibilityTimeout.fromSeconds(DefaultVisibilityTimeout),
          Duration.standardSeconds(DefaultDelay),
          Duration.standardSeconds(DefaultReceiveMessageWaitTimeSecondsAttribute),
          DateTime.now(),
          DateTime.now()
        )
        val actor = createQueueActor(nowProvider, queueData)
        queues(queueData.name) = actor
        result = queues.get(queueData.name)
      }

      result

    case ListQueues() => queues.keySet.toSeq
  }

  private def createQueueActor(nowProvider: NowProvider, queueData: QueueData): ActorRef = {
    val deadLetterQueueActor = queueData.deadLettersQueue.flatMap { qd =>
      queues.get(qd.name)
    }
    context.actorOf(Props(new QueueActor(nowProvider, queueData, deadLetterQueueActor)))
  }
}
