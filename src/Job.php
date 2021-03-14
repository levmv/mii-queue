<?php

namespace mii\queue;


abstract class Job
{
    public function __construct(array $config = [])
    {
        foreach ($config as $key => $value) {
            $this->$key = $value;
        }
    }

    abstract public function execute();


    /**
     * @param int        $attempt
     * @param \Throwable $error
     * @return bool
     */
    public function canRetry(int $attempt, \Throwable $error): bool
    {
        return $attempt <= 3;
    }

    /**
     * Minimum time (in seconds) before next attempt to process this job
     * @param int $attempt
     * @return int
     */
    public function getDelay(int $attempt): int
    {
        return round(min(
            rand(1, 5) + 3.5 ** $attempt,
            180
        ));
    }

}


