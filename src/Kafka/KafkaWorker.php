<?php

namespace Kim1ne\Kafka;

use Kim1ne\Core\EventLoopTrait;
use Kim1ne\Core\InputMessage;
use Kim1ne\Core\LooperInterface;
use RdKafka\Conf;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;

class KafkaWorker implements LooperInterface
{
    use EventLoopTrait;
    private int|float $timeoutMs = 0;

    private int $countErrorMessage = 0;
    public ?KafkaConsumer $consumer = null;
    private bool $sleep = true;

    private float|int $timeSleep = 5;

    public function __construct(public Conf $conf) {}

    public function setTimeSleep(float|int $seconds): self
    {
        $this->timeSleep = $seconds;
        return $this;
    }

    /**
     * @return KafkaConsumer
     */
    public function getConsumer(): KafkaConsumer
    {
        if (null === $this->consumer) {
            $this->consumer = new KafkaConsumer($this->conf);
        }

        return $this->consumer;
    }

    /**
     * @param array $topics
     * @return $this
     * @throws \RdKafka\Exception
     */
    public function subscribe(array $topics): static
    {
        $this->getConsumer()->subscribe($topics);
        return $this;
    }

    /**
     * @param int|float $timeoutMs
     * @return $this
     * time of waiting message from kafka
     */
    public function setTimeoutMs(int|float $timeoutMs): self
    {
        $this->timeoutMs = $timeoutMs;
        return $this;
    }

    /**
     * @param callable $callback
     * @return $this
     * this callback will be called if bad message
     *
     * @deprecated
     */
    public function error(callable $callback): static
    {
        $this->callbackError = $callback;
        return $this;
    }

    /**
     * @return $this
     * turns off the sleep mode
     * will be too many errors, the worker will continue the work
     */
    public function noSleep(): static
    {
        $this->sleep = false;
        return $this;
    }

    /**
     * @param callable $callback
     * @return $this
     * in this callback will be called, will be thrown out an exception
     *
     * @deprecated
     */
    public function critical(callable $callback): static
    {
        $this->callbackCritical = $callback;
        return $this;
    }

    /**
     * @return void
     * Stops the worker
     * if is parallel process, that  destroys the worker
     * and if he is last, stops the event-loop
     */
    public function stop(): void
    {
        if ($this->isRun() === false) {
            return;
        }

        $this->isRun = false;
        $this->getConsumer()->close();

        $this->stopLoop();
    }

    /**
     * @return void
     * The worker sleeps, if too many errors and turned on sleep-mode
     */
    public function sleep(): void
    {
        $timeSleep = $this->timeSleep;
        InputMessage::green('Too many errors, sleeping for ' . $timeSleep . ' seconds...');
        $this->countErrorMessage = 0;

        $this->isRun = false;
        $this->loop->addTimer($timeSleep, function () {
            $this->isRun = true;
            InputMessage::green('Resuming Kafka worker...');
        });
    }

    /**
     * @return void
     * Starts the sna starts loop
     * @see https://reactphp.org/event-loop/#loop
     */
    public function run(): void
    {
        $loop = $this->getLoop();

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

            $promise = $this->getConsumer()->consume($this->timeoutMs);

            $promise->then(function (Message $message) {
                if ($message->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {

                    if ($message->err === RD_KAFKA_RESP_ERR__TIMED_OUT) {
                        return;
                    }

                    ++$this->countErrorMessage;
                    $this->call('error', [$message]);

                    return;
                }

                $this->call('message', [$message, $this->getConsumer()]);
            })->otherwise(function (\Throwable $throwable) {
                $this->call('critical', [$throwable]);
            });
        });

        $this->runLoop();
    }

    public function getScopeName(): string
    {
        return 'kafka:worker';
    }
}