<?php

namespace Kim1ne\KafkaKit\Kafka;

use Kim1ne\KafkaKit\InputMessage;
use RdKafka\Conf;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class KafkaWorker
{
    private bool $isRun = false;
    private int|float $timeout_ms = 0;

    private int $countErrorMessage = 0;

    private readonly \Closure $callback;
    private readonly \Closure $callbackError;
    private readonly \Closure $callbackCritical;
    public KafkaConsumer $consumer;
    private bool $sleep = true;

    public function __construct(
        public Conf $conf,
        public ?LoopInterface $loop = null,
    )
    {
        $this->consumer = new KafkaConsumer($this->conf);
    }

    public function getConsumer(): KafkaConsumer
    {
        return $this->consumer;
    }

    public function subscribe(array $topics): static
    {
        $this->consumer->subscribe($topics);
        return $this;
    }

    public function setTimeoutMs(int|float $timeout_ms): self
    {
        $this->timeout_ms = $timeout_ms;
        return $this;
    }

    public function on(callable $callback): static
    {
        $this->callback = $callback;
        return $this;
    }

    public function error(callable $callback): static
    {
        $this->callbackError = $callback;
        return $this;
    }

    public function noSleep(): static
    {
        $this->sleep = false;
        return $this;
    }

    public function critical(callable $callback): static
    {
        $this->callbackCritical = $callback;
        return $this;
    }

    public function setLoop(LoopInterface $loop): static
    {
        if ($this->isRun) {
            throw new \RuntimeException('Kafka worker is already run.');
        }

        $this->loop = $loop;

        return $this;
    }

    public function __destruct()
    {
        $this->stop();
    }

    public function stop(): void
    {
        if ($this->isRun === false) {
            return;
        }

        $this->isRun = false;
        $this->consumer->close();

        if (!ParallelWorkers::isParalleled()) {
            $this->loop->stop();
        } else {
            ParallelWorkers::destroyWorker($this);
        }
    }

    private function sleep(): void
    {
        InputMessage::green('Too many errors, sleeping for 5 seconds...');
        $this->countErrorMessage = 0;

        $this->isRun = false;
        $this->loop->addTimer(5, function () {
            $this->isRun = true;
            InputMessage::green('Resuming Kafka worker...');
        });
    }

    public function run(): void
    {
        $this->isRun = true;
        $loop = $this->loop;

        if ($loop === null) {
            $loop = Loop::get();
            $this->loop = $loop;
        }

        if (!isset($this->callback)) {
            return;
        }

        $loop->addPeriodicTimer(0.2, function () {
            if ($this->countErrorMessage >= 20 && $this->sleep) {
                $this->sleep();
                return;
            }

            if (!$this->isRun) {
                return;
            }

            $promise = $this->consumer->consume($this->timeout_ms);

            $promise->then(function (Message $message) {
                $args = [$message, $this->consumer];

                if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {

                    if ($message->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
                        return;
                    }

                    ++$this->countErrorMessage;
                    if (isset($this->callbackError)) {
                        call_user_func($this->callbackError, $message);
                    }

                    return;
                }

                call_user_func_array($this->callback, [$message, $this->consumer]);
            })->otherwise(function (\Throwable $throwable) {
                if (isset($this->callbackCritical)) {
                    call_user_func($this->callbackCritical, $throwable);
                }
            });
        });
    }
}