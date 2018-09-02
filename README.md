# mii-queue

*Inspired by yii2-queue*

Installation
------------
```
composer require levmorozov/mii-queue
```

Basic Usage
-----------

Each task which is sent to queue should be defined as a separate class.

```php
class EmailJob extends Job 
{
    public $to;
    public $body;
    
    public function execute($queue)
    {
        Mii::$app->mailer()->to($this->to)->body($body)->send();
    }
}
```

Here's how to send a task into the queue:

```php
Mii::$app->emailqueue->push(new EmailJob([
    'to' => 'mail@yiiframework.com',
    'body' => 'Hi guys! Thank you for your framework!',
]));
```

To execute task you need to start console command:



```sh
# This command executes tasks in a loop until the queue is empty:
mii queue run --queue=emailqueue

# This command launches a daemon which infinitely queries the queue:
mii queue listen --queue=emailqueue
```