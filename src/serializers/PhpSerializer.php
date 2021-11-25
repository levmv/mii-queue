<?php

namespace mii\queue\serializers;


use mii\queue\Job;

class PhpSerializer implements SerializerInterface
{

    public function serialize(Job $job): string
    {
        return serialize($job);
    }


    public function unserialize(string $serialized): Job
    {
        return unserialize($serialized);
    }

}
