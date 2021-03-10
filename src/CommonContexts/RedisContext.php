<?php

namespace Proweb\CommonContexts;

use Behat\MinkExtension\Context\RawMinkContext;

class RedisContext extends RawMinkContext
{
    private $redis;

    public function __construct(\Redis $redis)
    {
        $this->redis = $redis;
    }

    /**
     * @Given I set redis key :key to :value
     */
    public function ISetRedisKeyTo(string $key, string $value): void
    {
        $this->redis->set($key, $value);
    }

    /**
     * @Given the Redis key :key should be equal to :expected
     */
    public function theRedisKeyShouldBeEqualTo(string $key, string $expected): void
    {
        if ($this->redis->get($key) !== $expected) {
            throw new \Exception(\sprintf('The redis key %s does not match expected %s', $key, $expected));
        }
    }
}
