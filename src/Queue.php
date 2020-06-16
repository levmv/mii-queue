<?php

namespace mii\queue;


use mii\queue\serializers\PhpSerializer;
use mii\queue\serializers\SerializerInterface;
use mii\core\Component;

abstract class Queue extends Component
{
    /**
     * @var SerializerInterface
     */
    public $serializer = PhpSerializer::class;

    public int $timeout = 3600;

    public $channel = '';

    public const STATUS_WAITING = 1;
    public const STATUS_LOCKED = 2;
    public const STATUS_DONE = 3;


    public function init(array $config = []): void
    {
        parent::init($config);
        $this->serializer = new $this->serializer;
    }

    /**
     * Pushes job into queue.
     *
     * @param Job $job
     * @return string|int|null id of a job message
     */
    abstract public function push(Job $job);

    /**
     * Find oldest free job and returns as array with keys: 'id', 'job', 'attempt'
     *
     * @return array
     */
    abstract public function fetch(): ?array;

    /**
     * Unlock task, increment number of attempts and set delay
     * @param     $id
     * @param int $delay
     */
    abstract public function free($id, $delay = 0): void;

    abstract public function freeExpired(): void;

    abstract public function remove($id): void;

    abstract public function clear(): void;

    /**
     * @param string $id of a job message
     * @return int status code
     */
    abstract public function status($id);

    /**
     * @param string $id of a job message
     * @return bool
     */
    public function isWaiting($id) : bool
    {
        return $this->status($id) === self::STATUS_WAITING;
    }

    /**
     * @param string $id of a job message
     * @return bool
     */
    public function isLocked($id) : bool
    {
        return $this->status($id) === self::STATUS_LOCKED;
    }

    /**
     * @param string $id of a job message
     * @return bool
     */
    public function isDone($id) : bool
    {
        return $this->status($id) === self::STATUS_DONE;
    }


    public function run(callable $can_continue, $repeat = false)
    {
        do {
            (mt_rand(1, 10) === 1) && $this->freeExpired();

            if ($payload = $this->fetch()) {

                $payload['status'] = 'successful';

                if ($this->execute($payload['id'], $payload['job'], $payload['attempt'], $payload['status'])) {
                    $this->remove($payload['id']);
                }
            } elseif (!$repeat) {
                break;
            }

        } while ($can_continue($payload));
    }

    public function execute($id, $job, $attempt, &$status): bool
    {
        $job = $this->serializer->unserialize($job);

        try {
            $job->execute();
        } catch (\Throwable $error) {

            \Mii::error($error, __METHOD__);

            if ($job->canRetry($attempt, $error)) {
                $this->free($id, $job->getDelay($attempt));
                $status = 'failed. unlocked for another try';
                return false;
            } else {
                $status = 'failed. no more attempts. removed';
            }
        }
        return true;
    }

}
