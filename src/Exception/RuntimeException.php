<?php declare(strict_types=1);

namespace Prooph\EventStore\Redis\Exception;

use Prooph\EventStore\Exception\RuntimeException as EventStoreRuntimeException;

class RuntimeException extends EventStoreRuntimeException implements RedisEventStoreException
{
}
