# Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require kim1ne/kafka-kit
```

## Usage

- [Kafka worker](#kafka-worker)
- [Start many workers](#start-many-workers)

#### Kafka Worker

This is wrap of library [RdKafka](https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/index.html). The library used libraries of [ReactPHP](https://reactphp.org/) for async.
```php
use Kim1ne\KafkaKit\InputMessage;
use Kim1ne\KafkaKit\Kafka\KafkaConsumer;
use Kim1ne\KafkaKit\Kafka\KafkaWorker;
use Kim1ne\KafkaKit\Kafka\Message;
use RdKafka\Conf;

$conf = new Conf();
$conf->set('metadata.broker.list', 'kafka:9092');
$conf->set('group.id', 'my-group');
// $conf->set(...) other settings

InputMessage::green('Start Worker');
$worker = new KafkaWorker($conf);

$worker
    ->on(function (Message $message, KafkaConsumer $consumer) {
        $consumer->commitAsync($message);
    })
    ->critical(function (\Throwable $throwable) {
        InputMessage::red('Error: ' . $throwable->getMessage());
    });

$worker->run();
```

### Start many workers
```php
\Kim1ne\KafkaKit\Kafka\ParallelWorkers::start(
    $worker1,
    $worker2,
    // ... $workerN
);
```
