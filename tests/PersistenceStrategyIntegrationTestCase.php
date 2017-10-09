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

use Prooph\Common\Messaging\FQCNMessageFactory;
use Prooph\EventStore\Redis\PersistenceStrategy;
use Prooph\EventStore\Redis\RedisEventStore;
use ProophTest\EventStore\AbstractEventStoreTest;
use Redis;

/**
 * @group integration
 */
abstract class PersistenceStrategyIntegrationTestCase extends AbstractEventStoreTest
{
    /** @var Redis */
    private $redisClient;

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

    abstract protected function getPersistenceStrategy(): PersistenceStrategy;
}
