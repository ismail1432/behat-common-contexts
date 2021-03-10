<?php

namespace Proweb\CommonContexts;

use Behat\Behat\Context\Context;
use Behat\Gherkin\Node\PyStringNode;
use Behat\Gherkin\Node\TableNode;
use Behatch\Json\Json;
use Behatch\Json\JsonInspector;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\DependencyInjection\ServiceLocator;
use Symfony\Component\EventDispatcher\EventDispatcherInterface;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Event\WorkerRunningEvent;
use Symfony\Component\Messenger\EventListener\StopWorkerOnMessageLimitListener;
use Symfony\Component\Messenger\MessageBusInterface;
use Symfony\Component\Messenger\Transport\AmqpExt\AmqpTransport;
use Symfony\Component\Messenger\Transport\AmqpExt\Connection;
use Symfony\Component\Messenger\Worker;
use Symfony\Component\Serializer\SerializerInterface;

class MessengerContext implements Context
{
    /** @var MessageBusInterface */
    private $bus;
    /** @var EventDispatcherInterface */
    private $eventDispatcher;
    /** @var SerializerInterface */
    private $serializer;
    /** @var ServiceLocator */
    private $receiversLocator;
    /** @var JsonInspector */
    private $jsonInspector;

    public function __construct(ContainerInterface $container)
    {
        $this->bus = $container->get(MessageBusInterface::class);
        $this->eventDispatcher = $container->get(EventDispatcherInterface::class);
        $this->serializer = $container->get(SerializerInterface::class);
        $this->receiversLocator = $container->get('messenger.receiver_locator');
        $this->jsonInspector = new JsonInspector('javascript');
    }

    /**
     * @BeforeScenario
     */
    public function beforeScenario(): void
    {
        $this->cleanQueues();
    }

    /**
     * @Given I purge the messenger queues
     */
    public function cleanQueues(): void
    {
        foreach (array_keys($this->receiversLocator->getProvidedServices()) as $transportName) {
            try {
                $transport = $this->getAmqpTransport($transportName);
            } catch (\Exception $e) {
                continue;
            }
            $connection = $this->getConnection($transport);
            $connection->purgeQueues();
        }
    }

    /**
     * @Given I purge the :queue messenger queue
     * @Given I purge the :queue messenger queue of the transport :transportName
     */
    public function cleanQueue(string $queue, ?string $transportName = null): void
    {
        $transportName = $transportName ?? $queue;

        $transport = $this->getAmqpTransport($transportName);

        $queues = $this->getQueues($transport);
        if (!isset($queues[$queue])) {
            throw new \LogicException(sprintf('The queue %s does not exist in the transport %s. Queues availables are: %s.', $queue, $transport, implode(', ', array_keys($queues))));
        }
        $queues[$queue]->purge();
    }

    /**
     * @When I consume :count message(s) in :queue messenger queue
     * @When I consume every message(s) in :queue messenger queue
     * @When I consume :count message(s) in :queue messenger queue of the transport :transportName
     * @When I consume every messages in :queue messenger queue of the transport :transportName
     */
    public function consumeMessagesInQueue(string $queue, ?int $count = null, ?string $transportName = null): void
    {
        $transportName = $transportName ?? $queue;

        $transport = $this->getAmqpTransport($transportName);

        // removes and adds StopWorkerOnMessageLimitListener (prevent multiple listeners for MessageLimit)
        $listeners = $this->eventDispatcher->getListeners(WorkerRunningEvent::class);
        foreach ($listeners as $listener) {
            if ($listener[0] instanceof StopWorkerOnMessageLimitListener) {
                $this->eventDispatcher->removeListener(WorkerRunningEvent::class, $listener);
            }
        }
        $totalMessages = $transport->getMessageCount();
        $count = $count ?? $totalMessages;
        if ($count < $totalMessages) {
            throw new \LogicException(sprintf('You asked to consume %d messages but there are %d.', $count, $totalMessages));
        }
        $this->eventDispatcher->addSubscriber(new StopWorkerOnMessageLimitListener($count));

        (new Worker([$transport], $this->bus, $this->eventDispatcher))->run();
    }

    /**
     * @Then the :queue messenger queue should be empty
     * @Then the :queue messenger queue of the transport :transportName should be empty
     * @Then the :queue messenger queue should have :count message(s)
     * @Then the :queue messenger queue of the transport :transportName should have :count message(s)
     */
    public function queueShouldHaveExpectedMessagesNumber(string $queue, int $count = 0, ?string $transportName = null): void
    {
        $transportName = $transportName ?? $queue;
        $transport = $this->getAmqpTransport($transportName);
        if ($count !== $countMessages = $transport->getMessageCount()) {
            throw new \LogicException(sprintf('There are %d messages in queue %s, but %d expected.', $countMessages, $queue, $count));
        }
    }

    /**
     * @Given I wait :seconds second(s) the :queue messenger queue to have :count message(s)
     * @Given I wait :seconds second(s) the :queue messenger queue of the transport :transportName to have :count message(s)
     */
    public function waitQueueToHaveExpectedMessagesNumber(string $queue, int $seconds, int $count, ?string $transportName = null): void
    {
        $transportName = $transportName ?? $queue;
        $transport = $this->getAmqpTransport($transportName);

        $i = 0;
        while ($count !== $transport->getMessageCount()) {
            usleep(1000000); // 1 second
            ++$i;
            if ($i > $seconds) {
                throw new \LogicException(sprintf('The queue %s have %s message, expected %s', $queue, $transport->getMessageCount(), $count));
            }
        }
    }

    /**
     * @Then the :queue messenger queue should have following messages:
     * @Then the :queue messenger queue of transport :transportName should have following messages:
     */
    public function queueShouldHaveFollowingMessages(string $queue, TableNode $tableNode, ?string $transportName = null): void
    {
        $transportName = $transportName ?? $queue;
        $transport = $this->getAmqpTransport($transportName);
        $messages = $this->getMessagesAsJson($transport);

        foreach ($tableNode->getRows() as $expectedMessage) {
            if (!\in_array($expectedMessage[0], $messages, true)) {
                $helper = PHP_EOL.'Current messages are : '.PHP_EOL.implode(PHP_EOL, $messages).PHP_EOL;

                throw new \LogicException(sprintf('Expected message "%s" not found. %s', $expectedMessage[0], $helper));
            }
        }
    }

    /**
     * @Then the :queue messenger queue should have messages that contain JSON nodes:
     * @Then the :queue messenger queue of transport :transportName should have messages that contain JSON nodes:
     */
    public function queueShouldHaveMessageThatContainFollowingJson(string $queue, TableNode $tableNode, ?string $transportName = null): void
    {
        $transport = $this->getAmqpTransport($transportName ?? $queue);
        $messages = $this->getMessagesAsJson($transport);
        $rows = $tableNode->getRowsHash();

        $found = false;
        foreach ($messages as $message) {
            $rowsNotFound = $rows;
            foreach ($rows as $node => $expected) {
                $actual = null;
                try {
                    $actual = $this->jsonInspector->evaluate(new Json($message), $node);
                } catch (\Throwable $e) {
                }

                if (null !== $actual && trim((string) $actual) === trim($expected)) {
                    unset($rowsNotFound[$node]);
                }
            }

            if ([] === $rowsNotFound) {
                $found = true;
                break;
            }
        }

        if (!$found) {
            throw new \LogicException(sprintf('Could not find a message that contains expected JSON: %s', json_encode($rows)));
        }
    }

    /**
     * @Then I dump messages in :queue messenger queue
     * @Then I dump messages in :queue messenger queue of transport :transportName
     */
    public function dumpMessages(string $queue, ?string $transportName = null): void
    {
        $transportName = $transportName ?? $queue;
        $transport = $this->getAmqpTransport($transportName);

        $messages = $this->getMessages($transport);
        dump($messages);
    }

    /**
     * @Given I publish in :queue messenger queue the following message:
     */
    public function publish(string $queue, TableNode $nodes): void
    {
        foreach ($nodes->getTable() as $row) {
            [$type, $payload] = $row;
            $message = $this->serializer->deserialize($payload, $type, 'json');
            $this->getAmqpTransport($queue)->send(new Envelope($message));
        }
    }

    /**
     * @Given I publish a :type message in :queue messenger queue with the following JSON:
     */
    public function publishAMessageWithJson(string $queue, string $type, PyStringNode $json): void
    {
        $message = $this->serializer->deserialize($json->getRaw(), $type, 'json');
        $this->getAmqpTransport($queue)->send(new Envelope($message));
    }

    private function getMessagesAsJson(AmqpTransport $transport): array
    {
        $messages = $this->getMessages($transport);

        return array_map(function ($message) {
            return $this->serializer->serialize($message, 'json');
        }, $messages);
    }

    private function getMessages(AmqpTransport $transport): array
    {
        $messages = [];
        $countMessages = $transport->getMessageCount();
        $counter = 0;
        foreach ($transport->get() as $envelope) {
            $messages[] = $envelope->getMessage();
            $transport->ack($envelope);
            $transport->send($envelope); // replace it in the queue
            ++$counter;
            if ($counter >= $countMessages) {
                break;
            }
        }

        return $messages;
    }

    private function getConnection(AmqpTransport $transport): Connection
    {
        $connection = (new ReflectionClass(AmqpTransport::class))->getProperty('connection');
        $connection->setAccessible(true);

        return $connection->getValue($transport);
    }

    /**
     * @return AMQPQueue[]
     */
    private function getQueues(AmqpTransport $transport): array
    {
        $res = [];
        foreach ($this->getConnection($transport)->getQueueNames() as $queueName) {
            $res[$queueName] = $this->getConnection($transport)->queue($queueName);
        }

        return $res;
    }

    private function getAmqpTransport(string $transportName): AmqpTransport
    {
        $transport = $this->receiversLocator->get($transportName);
        if (!$transport instanceof AmqpTransport) {
            throw new \LogicException(sprintf('The transport provided is not a %s but %s.', AmqpTransport::class, get_class($transport)));
        }

        return $transport;
    }
}
