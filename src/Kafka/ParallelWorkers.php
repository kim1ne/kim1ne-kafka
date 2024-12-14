<?php

namespace Kim1ne\KafkaKit\Kafka;

use Kim1ne\KafkaKit\InputMessage;
use React\EventLoop\Loop;

class ParallelWorkers
{
    private static bool $isParalleled = false;
    private static array $workers = [];

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

    public static function stop(): void
    {
        foreach (self::$workers as $worker) {
            self::destroyWorker($worker);
        }
    }

    private static function getHashWorker(KafkaWorker $worker): string
    {
        return spl_object_hash($worker);
    }

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

    public static function isParalleled(): bool
    {
        return self::$isParalleled;
    }
}