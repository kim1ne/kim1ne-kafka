<?php

namespace Kim1ne\Kafka;

use RdKafka\Conf;
use RdKafka\Message as KafkaMessage;
use RdKafka\Metadata\Broker;
use RdKafka\Producer;
use RdKafka\Topic;
use React\Promise\Promise;

class KafkaConsumer extends \RdKafka\KafkaConsumer
{
    private ?Producer $producer = null;

    /**
     * @param $timeoutMs
     * @return Promise
     *
     * returns the promise
     * @see https://reactphp.org/promise/
     */
    #[\ReturnTypeWillChange]
    public function consume($timeoutMs): Promise
    {
        return new Promise(function ($resolve, $reject) use ($timeoutMs) {
            try {
                $message = Message::fromRdKafka(parent::consume($timeoutMs));
                $resolve($message);
            } catch (\Throwable $throwable) {
                $reject($throwable);
            }
        });
    }

    /**
     * @param KafkaMessage|Message|array|null $messageOrOffset
     * @return void
     */
    public function commit(KafkaMessage|Message|array|null $messageOrOffset = null): void
    {
        $this->fromTheWrap('commit', $messageOrOffset);
    }

    /**
     * @param string $method
     * @param KafkaMessage|Message|array|null $messageOrOffset
     * @return void
     */
    private function fromTheWrap(string $method, KafkaMessage|Message|array|null $messageOrOffset = null): void
    {
        if ($messageOrOffset instanceof Message) {
            $messageOrOffset = $messageOrOffset->message;
        }

        parent::$method($messageOrOffset);
    }

    /**
     * @param KafkaMessage|Message|array|null $messageOrOffset
     * @return void
     */
    public function commitAsync(KafkaMessage|Message|array|null $messageOrOffset = null): void
    {
        $this->fromTheWrap('commitAsync', $messageOrOffset);
    }

    /**
     * @return string
     * @throws \RdKafka\Exception
     *
     * Returns all brokers that the consumer is listening to
     */
    public function getBrokers(): string
    {
        /**
         * @var Broker[] $brokers
         */
        $brokers = $this->getMetadata(true, null, 10_000)->getBrokers();

        $brokerList = [];
        foreach ($brokers as $broker) {
            $brokerList[] = $broker->getHost() . ':' . $broker->getPort();
        }

        return implode(',', $brokerList);
    }

    /**
     * @param string|null $transactionalId
     * @param int|null $timeWaiting
     * @return Producer
     * @throws \RdKafka\Exception
     */
    private function createProducer(?string $transactionalId = null, ?int $timeWaiting = null): Producer
    {
        if ($this->producer !== null) {
            return $this->producer;
        }

        $conf = new Conf();
        $conf->set('log_level', (string) LOG_DEBUG);
        $conf->set('bootstrap.servers', $this->getBrokers());

        $isTransaction = $transactionalId !== null;

        if ($isTransaction) {
            $conf->set('transactional.id', $transactionalId);
        }

        $producer = new Producer($conf);

        if ($isTransaction) {
            $this->producer = $producer;

            $this->producer->initTransactions($timeWaiting ?? 0);
        }

        return $producer;
    }

    /**
     * @param Message $message
     * @param string|null $overrideTopicName
     * @param int $timeWaiting
     * @return void
     * @throws \RdKafka\Exception
     * @throws \Throwable
     *
     * commits current message
     * creates duplicate the message, increments attempt on and sends to the end the topic
     *
     * the topic may be specified, otherwise will be selected the topic of the message
     */
    public function retry(Message $message, ?string $overrideTopicName = null, int $timeWaiting = 10_000): void
    {
        $message->incrementAttempts();

        $producer = $this->createProducer('transaction-message-' . $message->offset, $timeWaiting);

        $topic = $producer->newTopic($overrideTopicName ?? $message->topic_name);

        try {
            $producer->beginTransaction();

            $this->sendDuplicateMessage($topic, $message);

            $this->commitAsync($message);

            $producer->commitTransaction($timeWaiting);
        } catch (\Throwable $e) {
            $producer->abortTransaction($timeWaiting);
            throw $e;
        }
    }

    private function sendDuplicateMessage(Topic $topic, Message $message): void
    {
        $topic->producev(
            $message->partition,
            0,
            $message->payload,
            $message->key,
            $message->headers
        );
    }
}