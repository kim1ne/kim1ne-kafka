<?php

namespace Kim1ne;

use React\EventLoop\LoopInterface;

interface Looper
{
    public function setLoop(LoopInterface $loop): static;

    public function getLoop(): LoopInterface;

    public function run(): void;

    public function stop(): void;
}