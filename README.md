# Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require kim1ne/kafka-kit
```

## Usage

- [Kafka worker](#kafka-worker)
- [Start many workers](#start-many-workers)
- [API](#api)

#### Kafka Worker

This is wrap of library [RdKafka](https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/index.html). The library used libraries of [ReactPHP](https://reactphp.org/) for async.
Not locks stream.
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

### Start many workers
This functional started [event loop](https://reactphp.org/event-loop/#usage) and lock stream.
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
        // except messages with error code RD_KAFKA_RESP_ERR__TIMED_OUT 
    });
```

Stops the worker
```php
$worker->stop();
```
Set timeout for call method of RdKafka\Consumer::consume($timeout_ms)
```php
$worker->setTimeoutMs(1000); // default 0
```
Will be return RdKafka\Consumer
```php
$consumer = $worker->getConsumer();
```

This method turn off sleep the worker, otherwise after 20 callbacks ->error the worker go to sleep for 5 seconds
```php
$worker->noSleep();
```