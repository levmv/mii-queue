<?php

namespace levmorozov\queue\serializers;


use levmorozov\queue\Job;

class PhpSerializer implements SerializerInterface
{

    public function serialize(Job $job)
    {
        return serialize($job);
    }


    public function unserialize(string $serialized): Job
    {
        return unserialize($serialized);
    }

}