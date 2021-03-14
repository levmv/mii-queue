<?php

namespace mii\queue\console;

use mii\console\Controller;

class Queue extends Controller
{
    private static bool $exit = false;

    private int $count = 0;

    private string $queue = 'queue';
    private int $interval = 250000;
    private int $interval_idle = 4;
    private bool $verbose = false;
    private bool $isolate = false;


    protected function before()
    {
        $exit_signals = [
            15, // SIGTERM
            2,  // SIGINT
            1,  // SIGHUP
        ];

        if (extension_loaded('pcntl')) {
            foreach ($exit_signals as $signal) {
                pcntl_signal($signal, static function () {
                    static::$exit = true;
                });
            }
        }

        $this->queue = (string) $this->request->param('queue', $this->queue);
        $this->interval = (int) $this->request->param('interval', $this->interval);
        $this->interval_idle = (int) $this->request->param('interval_idle', $this->interval_idle);
        $this->verbose = (bool) $this->request->param('verbose', $this->verbose);
        $this->isolate = (bool) $this->request->param('isolate', $this->isolate);

        return parent::before();
    }


    /**
     * Print usage information
     */
    public function help()
    {
        $this->info(
            "\nUsage: mii queue (run|listen) [options]\n\n" .
            "Options:\n" .
            " --queue=<name>\tQueue component name\n" .
            " ——interval=<n>\tTime (in microseconds) before each job. Default: {$this->interval}\n" .
            " ——interval_idle=<n>\tTime (in seconds) before each check for emptiness of queue. Default: {$this->interval_idle}\n" .
            " ——verbose \tEnable verbose output\n" .
            " ——isolate \tExecute jobs in different process each\n" .
            "\n\n");
    }

    /**
     * Executes tasks in a loop until the queue is empty
     */
    public function run()
    {
        $this->process(false);
        if($this->verbose || $this->count) {
            $this->info("Finished. :n jobs processed.", [":n" => $this->count]);
        }
    }

    /**
     * Launches a daemon which infinitely queries the queue
     */
    public function listen()
    {
        $this->process(true);
    }


    public function clear()
    {
        \Mii::$app->get($this->queue)->clear();
    }


    public function stat()
    {
        $queue = \Mii::$app->get($this->queue);
        $stat = $queue->stat();
        $this->info("Total jobs: {$stat['total']}, locked: {$stat['locked']}");
    }


    private function process($repeat)
    {
        if (!\Mii::$app->has($this->queue)) {
            $this->error("Unknown component: {$this->queue}");
        }


        $queue = \Mii::$app->get($this->queue);

        if (!$queue instanceof \mii\queue\Queue) {
            $this->error("Wrong class: {$this->queue}");
        }

        $queue->run(function ($data) {

            if (empty($data)) {
                sleep($this->interval_idle);
            } else {
                $this->count++;

                $this->info("[:date] Job #:id status: :status", [
                    ':id' => $data['id'],
                    ':status' => $data['status'],
                    ':date' => date('d.m H:i:s')
                ]);

                usleep($this->interval);
            }

            return $this->checkSignals();
        }, $repeat);
    }


    private function checkSignals() : bool
    {
        if (extension_loaded('pcntl')) {
            pcntl_signal_dispatch();
        }
        return !static::$exit;
    }

}
