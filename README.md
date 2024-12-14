# Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require kim1ne/kafka-kit
```

## Usage

- [Kafka worker](#kafka-worker)
- [Launch several of workers](#launch-several-of-workers)
- [API](#api)

#### Kafka Worker

This is wrap of the library [RdKafka](https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/index.html). The library uses libraries of the [ReactPHP](https://reactphp.org/) for async.
Stream doesn't locks.
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

$worker->subscribe(['my-topic'])

$worker
    ->on(function (Message $message, KafkaConsumer $consumer) {
        $consumer->commitAsync($message);
    })
    ->critical(function (\Throwable $throwable) {
        InputMessage::red('Error: ' . $throwable->getMessage());
    });

$worker->run();
```

### Launch several of workers
The functional starts [event loop](https://reactphp.org/event-loop/#usage) and locks stream.
```php
\Kim1ne\KafkaKit\Kafka\ParallelWorkers::start(
    $worker1,
    $worker2,
    // ... $workerN
);
```

### API
```php
use Kim1ne\KafkaKit\Kafka\Message;

$worker
    ->error(function (Message $message) {
        // callback for bad message
        // $message->err !== RD_KAFKA_RESP_ERR_NO_ERROR
        // except messages with error code === RD_KAFKA_RESP_ERR__TIMED_OUT 
    });
```

Stops the worker
```php
$worker->stop();
```
Sets timeout for call method of the RdKafka\Consumer::consume($timeout_ms)
```php
$worker->setTimeoutMs(1000); // default 0
```
Returns object of the RdKafka\Consumer:::class
```php
$consumer = $worker->getConsumer();
```

Disables the sleep mode
```php
$worker->noSleep();
```