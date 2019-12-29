<?php

namespace levmorozov\queue\drivers;


use levmorozov\queue\Job;
use levmorozov\queue\Queue;
use mii\db\DB;
use mii\db\Expression;
use mii\db\Query;

class Mysql extends Queue
{

    public $table = 'queue';

    public function push(Job $job, $delay = 0): bool
    {
        return (bool)(new Query())
            ->insert($this->table)
            ->columns(['channel', 'updated', 'delay', 'locked', 'attempt', 'job'])
            ->values([
                $this->channel,
                time(),
                $delay,
                0,
                0,
                $this->serializer->serialize($job)
            ])
            ->execute();
    }


    public function fetch(): ?array
    {
        $result = null;

        try {
            DB::begin();

            $result = (new Query())
                ->select(['id', new Expression('(`updated` + `delay`) AS `age`'), 'job', 'attempt'])
                ->for_update()
                ->from($this->table)
                ->where('channel', '=', $this->channel)
                ->and_where('locked', '=', 0)
                ->having('age', '<=', time())
                ->order_by('age', 'asc')
                ->one();

            if ($result !== null) {
                (new Query())
                    ->update($this->table)
                    ->set([
                        'locked' => 1,
                        'updated' => time()
                    ])
                    ->where('id', '=', $result['id'])
                    ->execute();
            }

            DB::commit();

        } catch (\Throwable $t) {
            DB::rollback();
        }

        return $result;
    }

    public function free($id, $delay = 0): void
    {
        (new Query())
            ->update($this->table)
            ->set([
                'locked' => 0,
                'delay' => $delay,
                'attempt' => new Expression('attempt + 1'),
                'updated' => time()
            ])
            ->where('id', '=', $id)
            ->execute();
    }


    public function remove($id): void
    {
        (new Query())
            ->delete($this->table)
            ->where('id', '=', $id)
            ->execute();
    }

    public function clear(): void
    {
        (new Query())
            ->delete($this->table)
            ->where('channel', '=', $this->channel)
            ->execute();
    }


    public function free_expired(): void
    {
        (new Query())
            ->update($this->table)
            ->set([
                'locked' => 0,
                'updated' => time()
            ])
            ->where('locked', '=', 1)
            ->and_where('updated', '<', time() - $this->timeout)
            ->execute();
    }


}