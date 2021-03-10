<?php

namespace Proweb\CommonContexts;

use Behat\Behat\Context\Context;
use Behat\Gherkin\Node\TableNode;
use OldSound\RabbitMqBundle\RabbitMq\Consumer;
use OldSound\RabbitMqBundle\RabbitMq\Producer;
use PhpAmqpLib\Message\AMQPMessage;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Tests\Behat\AMQPLazyConnection;

abstract class AbstractRabbitMQContext implements Context
{
    private $container;

    /** @var AMQPLazyConnection */
    private $rabbitMqConnection;

    public function __construct(ContainerInterface $container)
    {
        $this->container = $container;
        $this->rabbitMqConnection = $this->container->get('old_sound_rabbit_mq.connection.default');
    }

    abstract public function countMessage(string $producer, bool $deadLetter = false): ?int;

    /**
     * @Given I clean the :queue queue
     */
    public function iCleanTheQueue($queue): void
    {
        $this->container->get(\sprintf('old_sound_rabbit_mq.%s_consumer', $queue))->purge($queue);
    }

    /**
     * @Then print :queue messages
     */
    public function printProducerMessages(string $queue): void
    {
        $messages = $this->getMessages($queue);
        if (!\count($messages)) {
            echo 'No message';

            return;
        }

        $exchange = null;
        /** @var AMQPMessage $message */
        foreach ($messages as $message) {
            if ($exchange !== $newExchange = $this->getMessageValue($message, 'exchange')) {
                $exchange = $newExchange;
                echo \sprintf("exchange: %s\n", $exchange);
            }
            if ($routingKey = $this->getMessageValue($message, 'routing_key')) {
                echo \sprintf("routing key: %s\n", $routingKey);
            }
            echo \sprintf("body: %s\n", $this->getMessageValue($message, 'body'));
        }
    }

    /**
     * @Then :queue should have :number message(s)
     */
    public function producerShouldHavePublishedMessages(string $queue, int $number): void
    {
        $deadLetter = \preg_match('/_dl$/', $queue);
        $messagesCount = $this->countMessage($queue, $deadLetter);

        if ($messagesCount !== $number) {
            $plural = $messagesCount < 2 ? 'message' : 'messages';
            $errorMessage = \sprintf('%d %s published, but should be %d', $messagesCount, $plural, $number);

            if (!$deadLetter) {
                $this->printProducerMessages($queue);
            }

            throw new \LogicException($errorMessage);
        }
    }

    /**
     * @Then :queue should have message(s) below:
     */
    public function producerShouldHavePublishedMessagesBelow(string $queue, TableNode $tableNode): void
    {
        $messages = $this->getMessages($queue);
        $bodyData = \array_map(function (AMQPMessage $message) {
            return $message->getBody();
        }, $messages);

        foreach ($tableNode->getRows() as $expectedMessage) {
            if (!\in_array($expectedMessage[1], $bodyData, true)) {
                $helper = PHP_EOL.'Current messages are : '.PHP_EOL.\implode(PHP_EOL, $bodyData);

                throw new \LogicException(\sprintf('Expected message "%s" not found. %s', $expectedMessage[1], $helper));
            }
        }
    }

    /**
     * Be careful, this consume the message(s).
     *
     * @Then :queue consume message(s) below:
     */
    public function producerConsumeMessagesBelow(string $queue, TableNode $tableNode): void
    {
        if (!$consumer = $this->getConsumer($queue)) {
            return;
        }

        $channel = $this->rabbitMqConnection->channel();
        $messages = [];
        /** @var AMQPMessage $message */
        while ($message = $channel->basic_get($queue)) {
            $message->delivery_info['channel'] = $channel;
            $messages[$message->getBody()] = $message;
        }

        foreach ($tableNode->getRowsHash() as $expectedMessage) {
            if (!\array_key_exists($expectedMessage, $messages)) {
                $currentMessages = [];
                foreach ($messages as $key => $message) {
                    if ($message instanceof AMQPMessage) {
                        $currentMessages[] = $message->body;
                    } else {
                        $currentMessages[] = $message;
                    }
                }

                $helper = PHP_EOL.'Current messages are : '.PHP_EOL.\implode(PHP_EOL, $currentMessages);

                throw new \LogicException(\sprintf('Expected message "%s" not found. %s', $expectedMessage, $helper));
            }
            $consumer->processMessage($messages[$expectedMessage]);
        }
    }

    /**
     * @When consumer :consumer consumes :count message(s)
     */
    public function consumerConsumesMessages(string $consumer, int $count): void
    {
        $this->getConsumer($consumer)->consume($count);
    }

    /**
     * @Then I wait the :queue to have :expectedCount message(s)
     */
    public function waitConsumeMessage(string $queue, int $expectedCount): void
    {
        $count = $this->countMessage($queue);
        $i = 0;

        while ($count !== $expectedCount) {
            \usleep(100000); // 0.1 second
            ++$i;
            $count = $this->countMessage($queue);

            // TTL of 3 seconds
            if ($i > 30) {
                throw new \LogicException(\sprintf('The queue %s have %s message, expected %s', $queue, $count, $expectedCount));
            }
        }
    }

    /**
     * @Then I publish the following message(s) to the :queue queue:
     */
    public function iPublishTheFollowingMessageToTheQueue(string $queue, TableNode $tableNode): void
    {
        $messages = \array_keys($tableNode->getRowsHash());
        foreach ($messages as $message) {
            $this->getProducer($queue)->publish($message);
        }
    }

    private function getProducer(string $producer): ?Producer
    {
        $serviceId = \sprintf('old_sound_rabbit_mq.%s_producer', $producer);

        if ($this->container->has($serviceId)) {
            return $this->container->get($serviceId);
        }

        throw new LogicException(\sprintf('The container does not contains any %s service.', $serviceId));
    }

    private function getConsumer(string $consumer): ?Consumer
    {
        $serviceId = \sprintf('old_sound_rabbit_mq.%s_consumer', $consumer);

        if ($this->container->has($serviceId)) {
            return $this->container->get($serviceId);
        }

        throw new LogicException(\sprintf('The container does not contains any %s service.', $serviceId));
    }

    public function getMessages(string $queue): ?array
    {
        $channel = $this->rabbitMqConnection->channel();
        $messages = [];

        /** @var AMQPMessage $message */
        while ($message = $channel->basic_get($queue)) {
            $messages[] = $message;
        }

        if (!$producer = $this->getProducer($queue)) {
            return null;
        }

        // Republish messages because getting them is consuming them.
        foreach ($messages as $message) {
            $producer->publish($message->getBody());
        }

        return $messages;
    }

    private function getMessageValue(AMQPMessage $message, string $key)
    {
        switch ($key) {
            case 'exchange':
                return $message->delivery_info['exchange'];
            case 'routing_key':
                return $message->delivery_info['routing_key'];
            case 'body':
                return $message->getBody();
            default:
                return $message->get($key);
        }
    }
}
