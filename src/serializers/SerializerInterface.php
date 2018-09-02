<?php

namespace levmorozov\queue\serializers;


use levmorozov\queue\Job;

interface SerializerInterface
{

    public function serialize(Job $job);

    public function unserialize(string $serialized): Job;
}