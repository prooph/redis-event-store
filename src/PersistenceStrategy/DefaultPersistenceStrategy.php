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

namespace Prooph\EventStore\Redis\PersistenceStrategy;

use Prooph\EventStore\Redis\PersistenceStrategy;
use Prooph\EventStore\StreamName;

final class DefaultPersistenceStrategy implements PersistenceStrategy
{
    public function getEventStreamHashKey(StreamName $streamName): string
    {
        return sprintf('event_streams:{%s}', $this->getStreamNameHash($streamName));
    }

    private function getStreamNameHash(StreamName $streamName): string
    {
        return sha1($streamName->toString());
    }
}
