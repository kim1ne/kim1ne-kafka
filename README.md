# Installation

This package can be installed as a [Composer](https://getcomposer.org/) dependency.

```bash
composer require kim1ne/kafka
```

## Usage

- [Kafka worker](#kafka-worker)
- [Launch several of workers](#launch-several-of-workers)
- [API](#api)

#### Kafka Worker

This is wrap of the library [RdKafka](https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/index.html). The library uses libraries of the [ReactPHP](https://reactphp.org/) for async.
Stream doesn't lock.
```php
use Kim1ne\InputMessage;
use Kim1ne\Kafka\KafkaConsumer;
use Kim1ne\Kafka\KafkaWorker;
use Kim1ne\Kafka\Message;
use RdKafka\Conf;

$conf = new Conf();
$conf->set('metadata.broker.list', 'kafka:9092');
$conf->set('group.id', 'my-group');
// $conf->set(...) other settings

$worker = new KafkaWorker($conf);

$worker->subscribe(['my-topic'])

$worker
    ->on(function (Message $message, KafkaConsumer $consumer) {
        $consumer->commitAsync($message);
    })
    ->critical(function (\Throwable $throwable) {
        InputMessage::red('Error: ' . $throwable->getMessage());
    });

InputMessage::green('Start Worker');

$worker->run();
```

### Launch several of workers
The functional starts [event loop](https://reactphp.org/event-loop/#usage) and locks stream.
```php
use Kim1ne\InputMessage;
use Kim1ne\Kafka\KafkaConsumer;
use Kim1ne\Kafka\Message;
/**
 * @var \RdKafka\Conf $conf 
 */
\Kim1ne\Kafka\ParallelWorkers::start(
    (new \Kim1ne\Kafka\KafkaWorker($conf))
        ->subscribe(['topic-1'])
        ->on(function (Message $message, KafkaConsumer $consumer) {
            InputMessage::red('Message in the first worker!')
        }),
    (new \Kim1ne\Kafka\KafkaWorker($conf))
        ->subscribe(['topic-2'])
        ->on(function (Message $message, KafkaConsumer $consumer) {
            InputMessage::red('Message in the second worker!')
        }),
    // ... $workerN
);
```

### API
This callback will be called on message from the kafka
```php
use Kim1ne\Kafka\Message;
use Kim1ne\Kafka\KafkaConsumer;

$worker
    ->on(function(Message $message, KafkaConsumer $consumer) {
        // Message! 
    });
```
This callback will be called if bad message
```php
use Kim1ne\Kafka\Message;

$worker
    ->error(function (Message $message) {
        // the callback for bad message
        // $message->err !== RD_KAFKA_RESP_ERR_NO_ERROR
        // except messages with error code === RD_KAFKA_RESP_ERR__TIMED_OUT 
    });
```

In this callback will be called, will be thrown out an exception
```php
$worker
    ->critical(function (\Throwable $e) {
        // Error
    })
```

Stops the worker. If is parallel process, that  destroys the worker and if he is last, stops the event-loop
```php
$worker->stop();
```
Sets timeout for call method of the [RdKafka\Consumer::consume($timeout_ms)](https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/rdkafka-kafkaconsumer.consume.html)
```php
$worker->setTimeoutMs(1000); // default is 0
```
Returns object of the [RdKafka\Consumer:::class](https://arnaud.le-blanc.net/php-rdkafka-doc/phpdoc/class.rdkafka-kafkaconsumer.html)
```php
$consumer = $worker->getConsumer();
```

turns off the sleep mode. Will be too many errors, the worker will continue the work
```php
$worker->noSleep();
```

Returns attempts of again processing
```php
/**
 * @var \Kim1ne\Kafka\Message $message 
 */
$message->getAttempts();
```

commits current message, creates duplicate the message, increments attempt on and sends to the end the topic. the topic may be specified, otherwise will be selected the topic of the message
```php
/**
 * @var \Kim1ne\Kafka\KafkaConsumer $consumer 
 */

$consumer->retry(Message $message, ?string $overrideTopicName = null, int $timeWaiting = 10_000);
```

```php
use Kim1ne\Kafka\Message;
use Kim1ne\Kafka\KafkaConsumer;

$worker
    ->on(function (Message $message, KafkaConsumer $consumer) {
        $attempts = $message->getAttempts();
        
        if ($attempts < 3) {
            $consumer->retry($message);
            return;
        }
        
        $consumer->commitAsync($message);
    });
```