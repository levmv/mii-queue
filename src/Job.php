<?php

namespace levmorozov\queue;


abstract class Job
{
    public function __construct(array $config = [])
    {
        foreach ($config as $key => $value)
            $this->$key = $value;
    }

    abstract public function execute();


    /**
     * @param int $attempt
     * @param \Throwable $error
     * @return bool
     */
    function can_retry(int $attempt, \Throwable $error): bool
    {
        return $attempt <= 3;
    }

    /**
     * Minimum time (in seconds) before next attempt to process this job
     * @param int $attempt
     * @return int
     */
    function get_delay($attempt): int
    {
        return round(min(
            mt_rand(1, 3) + pow(2, $attempt) * 2,
            60
        ));
    }

}


