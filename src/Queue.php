<?php

namespace levmorozov\queue;


use levmorozov\queue\serializers\PhpSerializer;
use levmorozov\queue\serializers\SerializerInterface;
use mii\core\Component;

abstract class Queue extends Component
{
    /**
     * @var SerializerInterface
     */
    public $serializer = PhpSerializer::class;

    public $timeout = 3600;

    public $channel = '';


    public function init(array $config = []): void
    {
        parent::init($config);
        $this->serializer = new $this->serializer;
    }


    abstract public function push(Job $job): bool;

    /**
     * Find oldest free job and returns as array with keys: 'id', 'job', 'attempt'
     *
     * @return array
     */
    abstract public function fetch(): ?array;

    /**
     * Unlock task, increment number of attempts and set delay
     * @param $id
     * @param $attempt
     * @param int $delay
     */
    abstract public function free($id, $delay = 0): void;

    abstract public function free_expired(): void;

    abstract public function remove($id): void;

    abstract public function clear(): void;


    public function run(callable $can_continue, $repeat = false)
    {
        do {
            (mt_rand(1, 10) === 1) && $this->free_expired();

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

            if ($job->can_retry($attempt, $error)) {
                $this->free($id, $attempt, $job->get_delay($attempt));
                $status = 'failed. unlocked for another try';
                return false;
            } else {
                $status = 'failed. no more attempts. removed';
            }
        }
        return true;
    }

}