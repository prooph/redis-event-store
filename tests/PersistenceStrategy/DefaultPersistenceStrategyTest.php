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

namespace ProophTest\EventStore\Redis\PersistenceStrategy;

use Prooph\EventStore\Redis\PersistenceStrategy;
use Prooph\EventStore\Redis\PersistenceStrategy\DefaultPersistenceStrategy;
use ProophTest\EventStore\Redis\PersistenceStrategyIntegrationTestCase;

/**
 * @covers \Prooph\EventStore\Redis\PersistenceStrategy\DefaultPersistenceStrategy
 */
final class DefaultPersistenceStrategyTest extends PersistenceStrategyIntegrationTestCase
{
    protected function getPersistenceStrategy(): PersistenceStrategy
    {
        return new DefaultPersistenceStrategy();
    }
}
