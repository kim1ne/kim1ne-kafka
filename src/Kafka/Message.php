<?php

namespace Kim1ne\KafkaKit\Kafka;

/**
 * @property $message
 * @property $err
 * @property $topic_name
 * @property $headers
 * @property $timestamp
 * @property $partition
 * @property $payload
 * @property $offset
 * @property $len
 * @property $key
 * @property $opaque
 * @method string errstr()
 */
class Message
{
    public readonly \RdKafka\Message $message;

    private function __construct() {}

    public static function fromRdKafka(\RdKafka\Message $message): static
    {
        $self = new static();
        $self->message = $message;
        return $self;
    }

    public function getAttempts(): int
    {
        return $this->message->headers['attempts'] ?? 1;
    }

    public function incrementAttempts(): void
    {
        $attempts = $this->getAttempts() + 1;
        $this->message->headers['attempts'] = $attempts;
    }

    public function __get(string $name)
    {
        return $this->message->$name ?? null;
    }

    public function __call(string $name, array $arguments)
    {
        return $this->message->$name(...$arguments);
    }

    public function __set(string $name, $value)
    {
        $this->message->$name = $value;
    }
}