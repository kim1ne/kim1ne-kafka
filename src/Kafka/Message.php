<?php

namespace Kim1ne\Kafka;

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
 *
 * This class all calls redirect in \RdKafka\Message::class
 */
class Message
{
    public readonly \RdKafka\Message $message;

    private function __construct() {}

    /**
     * @param \RdKafka\Message $message
     * @return static
     */
    public static function fromRdKafka(\RdKafka\Message $message): static
    {
        $self = new static();
        $self->message = $message;
        return $self;
    }

    /**
     * @return int
     * returns attempts for the message
     * default is 1
     */
    public function getAttempts(): int
    {
        return $this->message->headers['attempts'] ?? 1;
    }

    /**
     * @return void
     */
    public function incrementAttempts(): void
    {
        $attempts = $this->getAttempts() + 1;
        $this->message->headers['attempts'] = $attempts;
    }

    /**
     * @param string $name
     * @return null
     */
    public function __get(string $name)
    {
        return $this->message->$name ?? null;
    }

    /**
     * @param string $name
     * @param array $arguments
     * @return mixed
     */
    public function __call(string $name, array $arguments)
    {
        return $this->message->$name(...$arguments);
    }

    /**
     * @param string $name
     * @param $value
     * @return void
     */
    public function __set(string $name, $value)
    {
        $this->message->$name = $value;
    }
}