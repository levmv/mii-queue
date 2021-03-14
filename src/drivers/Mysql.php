<?php

namespace mii\queue\drivers;


use mii\queue\Job;
use mii\queue\Queue;
use mii\db\DB;
use mii\db\Expression;
use mii\db\Query;

class Mysql extends Queue
{
    public string $table = 'queue';

    public function push(Job $job, $delay = 0)
    {
        (new Query())
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

        return \Mii::$app->db->insertedId();
    }


    public function fetch(): ?array
    {
        $result = null;

        try {
            DB::begin();

            $result = (new Query())
                ->select(
                    'id',
                    new Expression('(`updated` + `delay`) AS `age`'), 'job', 'attempt'
                )
                ->forUpdate()
                ->from($this->table)
                ->where('channel', '=', $this->channel)
                ->andWhere('locked', '=', 0)
                ->having('age', '<=', time())
                ->orderBy('age', 'asc')
                ->orderBy('id', 'asc')
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

    public function stat(): array
    {
        return [
            'total' => (new Query())->from($this->table)->where('channel', '=', $this->channel)->count(),
            'locked' => (new Query())->from($this->table)->where('channel', '=', $this->channel)->where('locked', '=', 1)->count(),
        ];
    }


    public function freeExpired(): void
    {
        (new Query())
            ->update($this->table)
            ->set([
                'locked' => 0,
                'updated' => time()
            ])
            ->where('locked', '=', 1)
            ->andWhere('updated', '<', time() - $this->timeout)
            ->execute();
    }


    public function status($id)
    {
        $job = (new Query())
            ->select('id', 'locked')
            ->from($this->table)
            ->where('id', '=', $id)
            ->one();

        if (!$job)
            return self::STATUS_DONE;

        if ($job['locked'])
            return self::STATUS_LOCKED;

        return self::STATUS_WAITING;
    }


}
