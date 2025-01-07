<?php

namespace Kim1ne\Loop;

use Kim1ne\InputMessage;
use Kim1ne\Looper;
use React\EventLoop\Loop;

class LoopServer
{
    private static bool $start = false;

    public static function run(Looper ...$loopers): void
    {
        self::$start = true;
        $loop = Loop::get();

        foreach ($loopers as $stream) {
            $stream->setLoop($loop);
            $stream->run();
        }

        InputMessage::green('Start the Loop Server');

        register_shutdown_function(function ($loop) {
            $loop->stop();
        }, $loop);

        $loop->run();
    }

    public static function isStart(): bool
    {
        return self::$start;
    }
}