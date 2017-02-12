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
use ProophTest\EventStore\Mock\UserCreated;
use ProophTest\EventStore\Mock\UsernameChanged;
use Prophecy\Argument;
use Prophecy\Prophecy\ObjectProphecy;
use Redis;

/**
 * @group integration
 * @coversNothing
 */
final class RedisEventStoreIntegrationTest extends TestCase
{
    /** @var Redis */
    private $redisClient;

    /** @var ObjectProphecy|PersistenceStrategy */
    private $persistenceStrategy;

    /** @var RedisEventStore */
    private $eventStore;

    protected function setUp(): void
    {
        $this->redisClient = new Redis();
        $this->redisClient->connect(getenv('REDIS_HOST'), (int) getenv('REDIS_PORT'));
        $this->redisClient->setOption(Redis::OPT_PREFIX, 'proophTest:');

        $this->persistenceStrategy = $this->prophesize(PersistenceStrategy::class);
        $this->persistenceStrategy->getEventStreamHashKey(Argument::any())->willReturn('test_key');

        $this->eventStore = new RedisEventStore(
            $this->redisClient,
            $this->persistenceStrategy->reveal(),
            new FQCNMessageFactory()
        );
    }

    protected function tearDown(): void
    {
        $this->redisClient->flushDB();
    }

    /**
     * @test
     */
    public function it_appends_events_to_a_stream(): void
    {
        $this->eventStore->create($this->getTestStream());

        $streamEvent = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));

        $streamEvents = $this->eventStore->load(new StreamName('Prooph\Model\User'));

        $count = 0;
        $lastEvent = null;
        foreach ($streamEvents as $event) {
            $count++;
            $lastEvent = $event;
        }
        $this->assertEquals(2, $count);

        $this->assertInstanceOf(UsernameChanged::class, $lastEvent);
        $messageConverter = new NoOpMessageConverter();

        $streamEventData = $messageConverter->convertToArray($streamEvent);
        $lastEventData = $messageConverter->convertToArray($lastEvent);

        $this->assertEquals($streamEventData, $lastEventData);
    }

    /**
     * @test
     */
    public function it_loads_events_from_position(): void
    {
        $this->eventStore->create($this->getTestStream());

        $streamEvent1 = UsernameChanged::with(
            ['name' => 'John Doe'],
            2
        );

        $streamEvent1 = $streamEvent1->withAddedMetadata('tag', 'person');

        $streamEvent2 = UsernameChanged::with(
            ['name' => 'Jane Doe'],
            3
        );

        $streamEvent2 = $streamEvent2->withAddedMetadata('tag', 'person');

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'),
            new \ArrayIterator([$streamEvent1, $streamEvent2]));

        $streamEvents = $this->eventStore->load(new StreamName('Prooph\Model\User'), 2);

        $this->assertTrue($streamEvents->valid());
        $event = $streamEvents->current();
        $this->assertEquals(0, $streamEvents->key());
        $this->assertEquals('John Doe', $event->payload()['name']);

        $streamEvents->next();
        $this->assertTrue($streamEvents->valid());
        $event = $streamEvents->current();
        $this->assertEquals(1, $streamEvents->key());
        $this->assertEquals('Jane Doe', $event->payload()['name']);

        $streamEvents->next();
        $this->assertFalse($streamEvents->valid());
    }

    /**
     * @test
     */
    public function it_appends_events_to_stream_and_records_them(): void
    {
        $this->eventStore->create($this->getTestStream());

        $secondStreamEvent = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $this->eventStore->appendTo(new StreamName('Prooph\Model\User'), new ArrayIterator([$secondStreamEvent]));

        $this->assertCount(2, $this->eventStore->load(new StreamName('Prooph\Model\User')));
    }

    /**
     * @test
     */
    public function it_loads_events_from_number(): void
    {
        $stream = $this->getTestStream();

        $this->eventStore->create($stream);

        $streamEventVersion2 = UsernameChanged::with(
            ['new_name' => 'John Doe'],
            2
        );

        $streamEventVersion2 = $streamEventVersion2->withAddedMetadata('snapshot', true);

        $streamEventVersion3 = UsernameChanged::with(
            ['new_name' => 'Jane Doe'],
            3
        );

        $streamEventVersion3 = $streamEventVersion3->withAddedMetadata('snapshot', false);

        $this->eventStore->appendTo($stream->streamName(),
            new ArrayIterator([$streamEventVersion2, $streamEventVersion3]));

        $loadedEvents = $this->eventStore->load($stream->streamName(), 2);

        $this->assertCount(2, $loadedEvents);

        $loadedEvents->rewind();

        $this->assertTrue($loadedEvents->current()->metadata()['snapshot']);
        $loadedEvents->next();
        $this->assertFalse($loadedEvents->current()->metadata()['snapshot']);

        $streamEvents = $this->eventStore->load($stream->streamName(), 2);

        $this->assertCount(2, $streamEvents);

        $streamEvents->rewind();

        $this->assertTrue($streamEvents->current()->metadata()['snapshot']);
        $streamEvents->next();
        $this->assertFalse($streamEvents->current()->metadata()['snapshot']);
    }

    private function getTestStream(): Stream
    {
        $streamEvent = UserCreated::with(
            ['name' => 'Max Mustermann', 'email' => 'contact@prooph.de'],
            1
        );

        $streamEvent = $streamEvent->withAddedMetadata('tag', 'person');

        return new Stream(new StreamName('Prooph\Model\User'), new ArrayIterator([$streamEvent]));
    }
}
