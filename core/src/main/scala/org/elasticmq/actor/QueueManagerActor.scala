package org.elasticmq.actor

import org.elasticmq.msg._
import org.joda.time.{DateTime, Duration}
import scala.reflect._
import org.elasticmq.msg.DeleteQueue
import org.elasticmq.msg.CreateQueue
import akka.actor.{Props, ActorRef}
import org.elasticmq.util.{Logging, NowProvider}
import org.elasticmq.actor.queue.QueueActor
import org.elasticmq.{MillisVisibilityTimeout, QueueData, QueueAlreadyExists}
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

  val DefaultVisibilityTimeout = 30L
  val DefaultDelay = 0L
  val DefaultReceiveMessageWaitTimeSecondsAttribute = 0L

  private val queues = collection.mutable.HashMap[String, ActorRef]()

  def receiveAndReply[T](msg: QueueManagerMsg[T]): ReplyAction[T] = msg match {
    case CreateQueue(queueData) => {
      if (queues.contains(queueData.name)) {
        logger.debug(s"Cannot create queue, as it already exists: $queueData")
//        Left(new QueueAlreadyExists(queueData.name))

        //TODO flag for unsafe mode
        logger.info(s"Creating queue $queueData")
        Right(queues.get(queueData.name).getOrElse(createQueueActor(nowProvider, queueData)))
      } else if (!queueData.deadLettersQueue.forall(dlq => queues.contains(dlq.name))) {
        logger.debug(s"Cannot create queue, its dead letters queue doesnt exists: $queueData")
        Left(new QueueDoesNotExist(queueData.deadLettersQueue.get.name))
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

    case LookupQueue(queueName) => {
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
    }

    case ListQueues() => queues.keySet.toSeq
  }

  private def createQueueActor(nowProvider: NowProvider, queueData: QueueData): ActorRef = {
    val deadLetterQueueActor = queueData.deadLettersQueue.flatMap { qd =>
      queues.get(qd.name)
    }
    context.actorOf(Props(new QueueActor(nowProvider, queueData, deadLetterQueueActor)))
  }
}
