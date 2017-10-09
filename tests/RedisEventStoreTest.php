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
use Exception;
use PHPUnit\Framework\TestCase;
use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\EventStore;
use Prooph\EventStore\Exception\StreamExistsAlready;
use Prooph\EventStore\Exception\StreamNotFound;
use Prooph\EventStore\Exception\TransactionAlreadyStarted;
use Prooph\EventStore\Exception\TransactionNotStarted;
use Prooph\EventStore\Redis\Exception\RuntimeException;
use Prooph\EventStore\Redis\PersistenceStrategy;
use Prooph\EventStore\Redis\RedisEventStore;
use Prooph\EventStore\Stream;
use Prooph\EventStore\StreamName;
use Prophecy\Argument;
use Prophecy\Prophecy\ObjectProphecy;
use Redis;

/**
 * @coversDefaultClass \Prooph\EventStore\Redis\RedisEventStore
 * @group unit
 */
final class RedisEventStoreTest extends TestCase
{
    /** @var ObjectProphecy|Redis */
    private $redisClient;

    /** @var ObjectProphecy|PersistenceStrategy */
    private $persistenceStrategy;

    /** @var RedisEventStore */
    private $eventStore;

    protected function setUp(): void
    {
        $this->redisClient = $this->prophesize(Redis::class);
        $this->persistenceStrategy = $this->prophesize(PersistenceStrategy::class);

        $this->eventStore = new RedisEventStore(
            $this->redisClient->reveal(),
            $this->persistenceStrategy->reveal(),
            new FQCNMessageFactory()
        );
    }

    /**
     * @test
     * @covers ::__construct
     */
    public function it_creates_an_event_store_instance(): void
    {
        $this->assertInstanceOf(EventStore::class, $this->eventStore);
    }

    /**
     * @test
     * @covers ::fetchStreamMetadata
     */
    public function it_fetches_stream_metadata(): void
    {
        $streamName = new StreamName('test');
        $metadata = ['some' => 'metadata'];
        $encodedMetadata = json_encode($metadata);

        $this->persistenceStrategy->getEventStreamHashKey($streamName)->willReturn('test_key')->shouldBeCalled();
        $this->redisClient->hExists('test_key', 'realStreamName')->willReturn(true)->shouldBeCalled();
        $this->redisClient->hGet('test_key', 'metadata')->willReturn($encodedMetadata)->shouldBeCalled();

        $result = $this->eventStore->fetchStreamMetadata($streamName);

        $this->assertEquals($metadata, $result);
    }

    /**
     * @test
     * @covers ::fetchStreamMetadata
     */
    public function it_throws_exception_on_metadata_fetch_if_stream_not_exists(): void
    {
        $this->expectException(StreamNotFound::class);

        $streamName = new StreamName('test');

        $this->persistenceStrategy->getEventStreamHashKey($streamName)->willReturn('test_key');
        $this->redisClient->hExists('test_key', 'realStreamName')->willReturn(false)->shouldBeCalled();

        $this->eventStore->fetchStreamMetadata($streamName);
    }

    /**
     * @test
     * @covers ::updateStreamMetadata
     * @covers ::persistEventStreamMetadata
     */
    public function it_updates_stream_metadata(): void
    {
        $streamName = new StreamName('test');
        $newMetadata = ['some' => 'metadata'];
        $streamValues = [
            'realStreamName' => 'test',
            'metadata' => json_encode($newMetadata),
        ];

        $this->persistenceStrategy->getEventStreamHashKey($streamName)->willReturn('test_key');
        $this->redisClient->hExists('test_key', 'realStreamName')->willReturn(true)->shouldBeCalled();
        $this->redisClient->hMset('test_key', $streamValues)->willReturn(true)->shouldBeCalled();

        $this->eventStore->updateStreamMetadata($streamName, $newMetadata);
    }

    /**
     * @test
     * @covers ::updateStreamMetadata
     */
    public function it_throws_exception_on_metadata_update_if_stream_not_exists(): void
    {
        $this->expectException(StreamNotFound::class);

        $streamName = new StreamName('test');

        $this->persistenceStrategy->getEventStreamHashKey($streamName)->willReturn('test_key');
        $this->redisClient->hExists('test_key', 'realStreamName')->willReturn(false)->shouldBeCalled();

        $this->eventStore->updateStreamMetadata($streamName, []);
    }

    /**
     * @test
     * @covers ::updateStreamMetadata
     * @covers ::persistEventStreamMetadata
     */
    public function it_throws_exception_on_metadata_update_if_persisting_fails(): void
    {
        $this->expectException(RuntimeException::class);

        $streamName = new StreamName('test');

        $this->persistenceStrategy->getEventStreamHashKey($streamName)->willReturn('test_key');
        $this->redisClient->hExists('test_key', 'realStreamName')->willReturn(true)->shouldBeCalled();
        $this->redisClient->hMset('test_key', Argument::any())->willReturn(false)->shouldBeCalled();

        $this->eventStore->updateStreamMetadata($streamName, []);
    }

    /**
     * @test
     * @covers ::hasStream
     */
    public function it_returns_true_if_stream_exists(): void
    {
        $streamName = new StreamName('test');

        $this->persistenceStrategy->getEventStreamHashKey($streamName)->willReturn('test_key');
        $this->redisClient->hExists('test_key', 'realStreamName')->willReturn(true)->shouldBeCalled();

        $result = $this->eventStore->hasStream($streamName);

        $this->assertTrue($result);
    }

    /**
     * @test
     * @covers ::hasStream
     */
    public function it_returns_false_if_stream_not_exists(): void
    {
        $streamName = new StreamName('test');

        $this->persistenceStrategy->getEventStreamHashKey($streamName)->willReturn('test_key');
        $this->redisClient->hExists('test_key', 'realStreamName')->willReturn(false)->shouldBeCalled();

        $result = $this->eventStore->hasStream($streamName);

        $this->assertFalse($result);
    }

    /**
     * @test
     * @covers ::create
     */
    public function it_persists_stream_metadata_on_create(): void
    {
        $streamName = new StreamName('test');
        $metadata = ['some' => 'metadata'];
        $events = new ArrayIterator();
        $stream = new Stream($streamName, $events, $metadata);
        $streamValues = [
            'realStreamName' => 'test',
            'metadata' => json_encode($metadata),
        ];

        $this->persistenceStrategy->getEventStreamHashKey($streamName)->willReturn('test_key');
        $this->redisClient->hExists('test_key', 'realStreamName')->willReturn(false)->shouldBeCalled();
        $this->redisClient->hMset('test_key', $streamValues)->will(function (): bool {
            $this->hExists('test_key', 'realStreamName')->willReturn(true);

            return true;
        })->shouldBeCalled();

        $this->eventStore->create($stream);
    }

    /**
     * @test
     * @covers ::create
     */
    public function it_throws_exception_on_create_if_stream_already_exists(): void
    {
        $this->expectException(StreamExistsAlready::class);

        $streamName = new StreamName('test');
        $stream = new Stream($streamName, new ArrayIterator());

        $this->persistenceStrategy->getEventStreamHashKey($streamName)->willReturn('test_key');
        $this->redisClient->hExists('test_key', 'realStreamName')->willReturn(true)->shouldBeCalled();

        $this->eventStore->create($stream);
    }

    /**
     * @test
     * @covers ::updateStreamMetadata
     * @covers ::persistEventStreamMetadata
     */
    public function it_throws_exception_on_create_if_persisting_fails(): void
    {
        $this->expectException(RuntimeException::class);

        $streamName = new StreamName('test');
        $stream = new Stream($streamName, new ArrayIterator());

        $this->persistenceStrategy->getEventStreamHashKey($streamName)->willReturn('test_key');
        $this->redisClient->hExists('test_key', 'realStreamName')->willReturn(false);
        $this->redisClient->hMset('test_key', Argument::any())->willReturn(false)->shouldBeCalled();

        $this->eventStore->create($stream);
    }

    /**
     * @test
     * @covers ::beginTransaction
     */
    public function it_begins_transaction(): void
    {
        $this->redisClient->multi(Redis::MULTI)->shouldBeCalled();

        $this->eventStore->beginTransaction();
    }

    /**
     * @test
     * @covers ::beginTransaction
     */
    public function it_throws_exception_if_transaction_is_already_started(): void
    {
        $this->expectException(TransactionAlreadyStarted::class);

        $this->eventStore->beginTransaction();
        $this->eventStore->beginTransaction();
    }

    /**
     * @test
     * @covers ::commit
     */
    public function it_commits_transaction(): void
    {
        $this->redisClient->multi(Redis::MULTI)->willReturn($this->redisClient);
        $this->redisClient->exec()->shouldBeCalled();

        $this->eventStore->beginTransaction();
        $this->eventStore->commit();
    }

    /**
     * @test
     * @covers ::commit
     */
    public function it_throws_exception_on_commit_if_transaction_is_not_started(): void
    {
        $this->expectException(TransactionNotStarted::class);

        $this->eventStore->commit();
    }

    /**
     * @test
     * @covers ::commit
     */
    public function it_throws_exception_on_commit_if_transaction_is_already_committed(): void
    {
        $this->expectException(TransactionNotStarted::class);

        $this->redisClient->multi(Redis::MULTI)->willReturn($this->redisClient);
        $this->redisClient->exec()->shouldBeCalledTimes(1);

        $this->eventStore->beginTransaction();
        $this->eventStore->commit();
        $this->eventStore->commit();
    }

    /**
     * @test
     * @covers ::rollback
     */
    public function it_rolls_transaction_back(): void
    {
        $this->redisClient->multi(Redis::MULTI)->willReturn($this->redisClient);
        $this->redisClient->discard()->shouldBeCalled();

        $this->eventStore->beginTransaction();
        $this->eventStore->rollback();
    }

    /**
     * @test
     * @covers ::rollback
     */
    public function it_throws_exception_on_rollback_if_transaction_is_not_started(): void
    {
        $this->expectException(TransactionNotStarted::class);

        $this->eventStore->rollback();
    }

    /**
     * @test
     * @covers ::rollback
     */
    public function it_throws_exception_on_rollback_if_transaction_is_already_rolled_back(): void
    {
        $this->expectException(TransactionNotStarted::class);

        $this->redisClient->multi(Redis::MULTI)->willReturn($this->redisClient);
        $this->redisClient->discard()->shouldBeCalledTimes(1);

        $this->eventStore->beginTransaction();
        $this->eventStore->rollback();
        $this->eventStore->rollback();
    }

    /**
     * @test
     * @covers ::inTransaction
     */
    public function it_returns_true_if_in_transaction(): void
    {
        $this->eventStore->beginTransaction();

        $result = $this->eventStore->inTransaction();

        $this->assertTrue($result);
    }

    /**
     * @test
     * @covers ::inTransaction
     */
    public function it_returns_false_if_not_in_transaction(): void
    {
        $this->redisClient->multi(Redis::MULTI)->willReturn($this->redisClient);

        $this->assertFalse($this->eventStore->inTransaction());

        $this->redisClient->discard()->shouldBeCalled();
        $this->eventStore->beginTransaction();
        $this->eventStore->rollback();

        $this->assertFalse($this->eventStore->inTransaction());

        $this->redisClient->exec()->shouldBeCalled();
        $this->eventStore->beginTransaction();
        $this->eventStore->commit();

        $this->assertFalse($this->eventStore->inTransaction());
    }

    /**
     * @test
     * @covers ::transactional
     */
    public function it_executes_callable_transactional(): void
    {
        $callable = function ($eventStore) use (&$providedEventStore): string {
            $providedEventStore = $eventStore;

            return 'test_response';
        };

        $this->redisClient->multi(Redis::MULTI)->willReturn($this->redisClient);
        $this->redisClient->exec()->shouldBeCalled();

        $response = $this->eventStore->transactional($callable);

        $this->assertEquals($this->eventStore, $providedEventStore);
        $this->assertEquals('test_response', $response);
    }

    /**
     * @test
     * @covers ::transactional
     */
    public function it_rolls_back_if_transactional_exception_is_thrown(): void
    {
        $this->expectException(Exception::class);

        $callable = function (): void {
            throw new Exception();
        };

        $this->redisClient->multi(Redis::MULTI)->willReturn($this->redisClient);
        $this->redisClient->discard()->shouldBeCalled();

        $this->eventStore->transactional($callable);
    }
}
