<?php

namespace mii\queue\serializers;


use mii\queue\Job;

interface SerializerInterface
{

    public function serialize(Job $job);

    public function unserialize(string $serialized): Job;
}
