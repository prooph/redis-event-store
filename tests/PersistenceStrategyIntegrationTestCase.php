<?php
/**
 * This file is part of the prooph/redis-event-store.
 * (c) 2017 prooph software GmbH <contact@prooph.de>
 * (c) 2017 Sascha-Oliver Prolic <saschaprolic@googlemail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

declare(strict_types=1);

namespace ProophTest\EventStore\Redis;

use ArrayIterator;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\Common\Messaging\NoOpMessageConverter;
use Prooph\EventStore\Redis\PersistenceStrategy;
use Prooph\EventStore\Redis\RedisEventStore;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use ProophTest\EventStore\Mock\TestDomainEvent;
use Redis;

abstract class PersistenceStrategyIntegrationTestCase extends TestCase
{
    /** @var Redis */
    private $redisClient;

    /** @var RedisEventStore */
    private $eventStore;

    public function setUp(): void
    {
        $this->redisClient = new Redis();
        $this->redisClient->connect(getenv('REDIS_HOST'), (int) getenv('REDIS_PORT'));
        $this->redisClient->setOption(Redis::OPT_PREFIX, 'proophTest:');

        $this->eventStore = new RedisEventStore(
            $this->redisClient,
            $this->getPersistenceStrategy(),
            new FQCNMessageFactory()
        );
    }

    public function tearDown(): void
    {
        $this->redisClient->flushDB();
    }

    /**
     * @test
     */
    public function it_creates_a_stream_without_payload(): void
    {
        $streamName = new StreamName('test_stream');
        $stream = new Stream($streamName, new ArrayIterator());

        $this->assertFalse($this->eventStore->hasStream($streamName));
        $this->eventStore->create($stream);
        $this->assertTrue($this->eventStore->hasStream($streamName));

        $events = iterator_to_array($this->eventStore->load($streamName));

        $this->assertCount(0, $events);
        $this->assertEquals([], $this->eventStore->fetchStreamMetadata($streamName));
    }

    /**
     * @test
     * @dataProvider streamMetadata
     */
    public function it_creates_a_stream_with_metadata(array $streamMetadata): void
    {
        $streamName = new StreamName('test_stream');
        $stream = new Stream($streamName, new ArrayIterator(), $streamMetadata);

        $this->assertFalse($this->eventStore->hasStream($streamName));
        $this->eventStore->create($stream);
        $this->assertTrue($this->eventStore->hasStream($streamName));

        $metadata = $this->eventStore->fetchStreamMetadata($streamName);
        $this->assertEquals($streamMetadata, $metadata);
    }

    /**
     * @test
     * @dataProvider streamEvents
     */
    public function it_creates_a_stream_with_events(array $streamEvents): void
    {
        $streamName = new StreamName('test_stream');
        $stream = new Stream($streamName, new ArrayIterator($streamEvents));

        $this->assertFalse($this->eventStore->hasStream($streamName));
        $this->eventStore->create($stream);
        $this->assertTrue($this->eventStore->hasStream($streamName));

        $events = iterator_to_array($this->eventStore->load($streamName));
        $this->assertCount(count($streamEvents), $events);

        $messageConverter = new NoOpMessageConverter();
        foreach ($events as $key => $event) {
            $this->assertEquals(
                $messageConverter->convertToArray($streamEvents[$key]),
                $messageConverter->convertToArray($event)
            );
        }
    }

    /**
     * @test
     * @dataProvider streamEvents
     */
    public function it_appends_events_to_stream(array $streamEvents): void
    {
        $streamEventAmount = count($streamEvents);

        $streamName = new StreamName('test_stream');
        $stream = new Stream($streamName, new ArrayIterator($streamEvents));

        $this->eventStore->create($stream);

        $events = iterator_to_array($this->eventStore->load($streamName));
        $this->assertCount($streamEventAmount, $events);

        $newEvents = [
            TestDomainEvent::with(['some' => 'payload'], $streamEventAmount + 1),
            TestDomainEvent::with(['another' => 'payload'], $streamEventAmount + 2),
        ];

        $this->eventStore->appendTo($streamName, new ArrayIterator($newEvents));

        $events = iterator_to_array($this->eventStore->load($streamName));
        $this->assertCount($streamEventAmount + count($newEvents), $events);

        $messageConverter = new NoOpMessageConverter();
        $expectedEvents = array_merge($streamEvents, $newEvents);

        foreach ($events as $key => $event) {
            $this->assertEquals(
                $messageConverter->convertToArray($expectedEvents[$key]),
                $messageConverter->convertToArray($event)
            );
        }
    }

    /**
     * @test
     * @dataProvider streamEvents
     */
    public function it_loads_events_from_position(array $streamEvents): void
    {
        $streamName = new StreamName('test_stream');
        $stream = new Stream($streamName, new ArrayIterator($streamEvents));

        $this->eventStore->create($stream);

        $messageConverter = new NoOpMessageConverter();

        $streamEventAmount = count($streamEvents);
        $position = $streamEventAmount;

        do {
            $count = $streamEventAmount - $position + 1;

            $events = iterator_to_array($this->eventStore->load($streamName, $position));
            $this->assertCount($count, $events);

            $expectedEvents = array_values(
                array_intersect_key($streamEvents, array_fill($position - 1, $count, ''))
            );

            foreach ($events as $key => $event) {
                $this->assertEquals(
                    $messageConverter->convertToArray($expectedEvents[$key]),
                    $messageConverter->convertToArray($event)
                );
            }
        } while (--$position);
    }

    /**
     * @test
     * @dataProvider streamEvents
     */
    public function it_loads_events_from_position_with_count(array $streamEvents): void
    {
        $streamName = new StreamName('test_stream');
        $stream = new Stream($streamName, new ArrayIterator($streamEvents));

        $this->eventStore->create($stream);

        $messageConverter = new NoOpMessageConverter();

        $streamEventsAmount = count($streamEvents);
        $position = $streamEventsAmount;

        do {
            $count = min(2, $streamEventsAmount - $position + 1);

            $events = iterator_to_array($this->eventStore->load($streamName, $position, $count));
            $this->assertCount($count, $events);

            $expectedEvents = array_values(
                array_intersect_key($streamEvents, array_fill($position - 1, $count, ''))
            );

            foreach ($events as $key => $event) {
                $this->assertEquals(
                    $messageConverter->convertToArray($expectedEvents[$key]),
                    $messageConverter->convertToArray($event)
                );
            }
        } while (--$position);
    }

    public function streamMetadata(): \Generator
    {
        yield [[
        ]];

        yield [[
            'some' => 'metadata',
        ]];

        yield [[
            'some' => 'metadata',
            'foo' => ['bar', 'baz'],
        ]];
    }

    public function streamEvents(): \Generator
    {
        yield [[
            TestDomainEvent::with(['some' => 'payload'], 1),
        ]];

        yield [[
            TestDomainEvent::with(['some' => 'payload'], 1),
            TestDomainEvent::with(['some' => 'payload', 'fuu' => ['bar', 'baz']], 2),
        ]];

        yield [[
            TestDomainEvent::with(['some' => 'payload'], 1),
            TestDomainEvent::with(['some' => 'payload'], 2),
            TestDomainEvent::with(['some' => 'payload', 'fuu' => ['bar', 'baz']], 3),
        ]];

        yield [[
            TestDomainEvent::with(['some' => 'payload'], 1),
            TestDomainEvent::with(['some' => 'payload', 'fuu' => ['bar', 'baz']], 2),
            TestDomainEvent::with(['some' => 'payload'], 3),
            TestDomainEvent::with(['some' => 'payload', 'fuu' => ['bar' => ['baz', 1337]]], 4),
        ]];
    }

    abstract protected function getPersistenceStrategy(): PersistenceStrategy;
}
