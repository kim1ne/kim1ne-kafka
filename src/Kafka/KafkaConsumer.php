<?php

namespace Kim1ne\KafkaKit\Kafka;

use RdKafka\Conf;
use RdKafka\Message as KafkaMessage;
use RdKafka\Metadata\Broker;
use RdKafka\Producer;
use RdKafka\Topic;
use React\Promise\Promise;

class KafkaConsumer extends \RdKafka\KafkaConsumer
{
    private ?Producer $producer = null;

    #[\ReturnTypeWillChange]
    public function consume($timeout_ms): Promise
    {
        return new Promise(function ($resolve, $reject) use ($timeout_ms) {
            try {
                $message = Message::fromRdKafka(parent::consume($timeout_ms));
                $resolve($message);
            } catch (\Throwable $throwable) {
                $reject($throwable);
            }
        });
    }

    public function commit(KafkaMessage|Message|array|null $message_or_offsets = null): void
    {
        $this->fromTheWrap('commit', $message_or_offsets);
    }

    private function fromTheWrap(string $method, KafkaMessage|Message|array|null $message_or_offsets = null): void
    {
        if ($message_or_offsets instanceof Message) {
            $message_or_offsets = $message_or_offsets->message;
        }

        parent::$method($message_or_offsets);
    }

    public function commitAsync(KafkaMessage|Message|array|null $message_or_offsets = null): void
    {
        $this->fromTheWrap('commitAsync', $message_or_offsets);
    }

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