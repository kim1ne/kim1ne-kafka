<?php

namespace Kim1ne\Kafka;

use Kim1ne\Core\InputMessage;
use React\EventLoop\Loop;

/**
 * @deprecated
 */
class ParallelWorkers
{
    private static bool $isParalleled = false;
    private static array $workers = [];

    /**
     * @param KafkaWorker ...$workers
     * @return void
     *
     * The function runs workers and starts the event-loop
     * @see https://reactphp.org/event-loop/#usage
     */
    public static function start(KafkaWorker ...$workers): void
    {
        InputMessage::green('Starting workers...');

        $loop = Loop::get();

        foreach ($workers as $index => $worker) {
            $worker->setLoop($loop);
            $worker->run();

            InputMessage::green('Worker ' . ++$index . ' started.');

            $id = spl_object_hash($worker);
            self::$workers[$id] = $worker;
        }

        self::$isParalleled = true;

        InputMessage::green("All workers are running.");

        $loop->run();

        InputMessage::green("Loop finished.");
    }

    /**
     * @return void
     * Stops workers if started parallel process
     */
    public static function stop(): void
    {
        foreach (self::$workers as $worker) {
            self::destroyWorker($worker);
        }
    }

    /**
     * @param KafkaWorker $worker
     * @return string
     */
    private static function getHashWorker(KafkaWorker $worker): string
    {
        return spl_object_hash($worker);
    }

    /**
     * @param KafkaWorker $worker
     * @return void
     *
     * if will started parallel process
     * the worker stops
     *
     * if this is the last worker, that stops the event loop
     */
    public static function destroyWorker(KafkaWorker $worker): void
    {
        $hash = self::getHashWorker($worker);

        if(!empty(self::$workers[$hash])) {
            unset(self::$workers[$hash]);
        }

        if (count(self::$workers) === 0 && self::$isParalleled) {
            Loop::get()->stop();
            self::$isParalleled = false;
        }
    }

    /**
     * @return bool
     * the parallel process started?
     */
    public static function isParalleled(): bool
    {
        return self::$isParalleled;
    }
}