<?php

namespace Kim1ne\KafkaKit;

use Wujunze\Colors;

/**
 * @method static string green($message)
 * @method static string red($message)
 */
class InputMessage
{
    public static function getColoredString(string $message, ?string $color = null, ?string $background = null): void
    {
        echo (new Colors())->getColoredString($message . PHP_EOL, $color, $background);
    }

    public static function __callStatic(string $name, array $arguments): void
    {
        self::getColoredString($arguments[0] ?? '', $name);
    }
}