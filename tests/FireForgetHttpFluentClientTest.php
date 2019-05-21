<?php

namespace thamtechunit\fluentd;

use Yii;

class FireForgetHttpFluentClientTest extends \thamtechunit\fluentd\TestCase
{
    public $time;

    protected function setUp(): void
    {
        parent::setUp();
        $this->time = time();
    }

    public function testEmitSingle()
    {
        $client = Yii::createObject([
            'class' => MockClient::class,
        ]);

        $response = $client->emit('mytag', ['a' => 1, 'b' => 2], $this->time);

        $expectedCall = [
            'tag' => 'mytag',
            'records' => ['{"a":1,"b":2}'],
            'timestamp' => $this->time,
        ];

        $this->assertCount(1, $client->postCalls);
        $this->assertEquals(true, $response);
        $this->assertEquals($expectedCall, $client->postCalls[0]);
    }

    public function testEmitSingleFailure()
    {
        $client = Yii::createObject([
            'class' => MockClient::class,
        ]);

        $client->postResponse = [false];
        $response = $client->emit('mytag', ['a' => 1, 'b' => 2], $this->time);

        $expectedCall = [
            'tag' => 'mytag',
            'records' => ['{"a":1,"b":2}'],
            'timestamp' => $this->time,
        ];

        $this->assertCount(1, $client->postCalls);
        $this->assertEquals(false, $response);
        $this->assertEquals($expectedCall, $client->postCalls[0]);
    }

    public function testEmitSmallBatch()
    {
        $client = Yii::createObject([
            'class' => MockClient::class,
        ]);

        $client->postResponse = [true]; // one 'true' for each batch
        $records = [
            ['a' => 1, 'b' => 2],
            ['a' => 2, 'b' => 2],
        ];

        $response = $client->emitBatch('mytag', $records, $this->time);

        $expectedCall = [
            'tag' => 'mytag',
            'records' => ['[{"a":1,"b":2},{"a":2,"b":2}]'],
            'timestamp' => $this->time,
        ];

        $this->assertCount(1, $client->postCalls);
        $this->assertEquals([true, true], $response);
        $this->assertEquals($expectedCall, $client->postCalls[0]);
    }

    public function testEmitSmallBatchFailure()
    {
        $client = Yii::createObject([
            'class' => MockClient::class,
        ]);

        $client->postResponse = [false]; // one 'true' for each batch
        $records = [
            ['a' => 1, 'b' => 2],
            ['a' => 2, 'b' => 2],
        ];

        $response = $client->emitBatch('mytag', $records, $this->time);

        $expectedCall = [
            'tag' => 'mytag',
            'records' => ['[{"a":1,"b":2},{"a":2,"b":2}]'],
            'timestamp' => $this->time,
        ];

        $this->assertCount(1, $client->postCalls);
        $this->assertEquals([false, false], $response);
        $this->assertEquals($expectedCall, $client->postCalls[0]);
    }

    public function testEmitTwoConnectionBatches()
    {
        $client = Yii::createObject([
            'class' => MockClient::class,
            'batchSize' => 2,
            'batchesPerPost' => 2,
        ]);

        $client->postResponse = [true, true]; // one 'true' for each batch

        $connection1Batch1 = [
            0 => ['a' => 1, 'b' => 2],
            1 => ['a' => 2, 'b' => 2],
        ];
        $connection1Batch2 = [
            2 => ['a' => 3, 'b' => 2],
            3 => ['a' => 4, 'b' => 2],
        ];
        $connection2Batch1 = [
            4 => ['a' => 1, 'b' => 4],
            5 => ['a' => 2, 'b' => 4],
        ];
        $connection2Batch2 = [
            6 => ['a' => 3, 'b' => 4],
            7 => ['a' => 4, 'b' => 4],
        ];
        $records = array_merge($connection1Batch1,
            $connection1Batch2,
            $connection2Batch1,
            $connection2Batch2);

        $response = $client->emitBatch('mytag', $records, $this->time);

        $expectedCalls = [
            [
                'tag' => 'mytag',
                'records' => ['[{"a":1,"b":2},{"a":2,"b":2}]','[{"a":3,"b":2},{"a":4,"b":2}]'],
                'timestamp' => $this->time,
            ],
            [
                'tag' => 'mytag',
                'records' => ['[{"a":1,"b":4},{"a":2,"b":4}]','[{"a":3,"b":4},{"a":4,"b":4}]'],
                'timestamp' => $this->time,
            ],
        ];

        $this->assertCount(2, $client->postCalls);
        $this->assertEquals([true, true, true, true, true, true, true, true], $response);
        $this->assertEquals($expectedCalls, $client->postCalls);
    }

    public function testEmitTwoConnectionBatchesMidpointFailure()
    {
        $client = Yii::createObject([
            'class' => MockClient::class,
            'batchSize' => 2,
            'batchesPerPost' => 2,
        ]);

        $client->postResponse = [true, false]; // 'true' for batch one, 'false' for batch 2

        $r0 = ['a' => 1, 'b' => 2];
        $r1 = ['a' => 2, 'b' => 2];
        $r2 = ['a' => 3, 'b' => 2];
        $r3 = ['a' => 4, 'b' => 2];
        $r4 = ['a' => 1, 'b' => 4];
        $r5 = ['a' => 2, 'b' => 4];
        $r6 = ['a' => 3, 'b' => 4];
        $r7 = ['a' => 4, 'b' => 4];

        $connection1Batch1 = [
            0 => $r0,
            1 => $r1,
        ];
        $connection1Batch2 = [
            2 => $r2,
            3 => $r3,
        ];
        $connection2Batch1 = [
            4 => $r4,
            5 => $r5,
        ];
        $connection2Batch2 = [
            6 => $r6,
            7 => $r7,
        ];
        $records = array_merge($connection1Batch1,
            $connection1Batch2,
            $connection2Batch1,
            $connection2Batch2);

        $response = $client->emitBatch('mytag', $records, $this->time);

        $expectedCalls = [
            [
                'tag' => 'mytag',
                'records' => ['[{"a":1,"b":2},{"a":2,"b":2}]','[{"a":3,"b":2},{"a":4,"b":2}]'],
                'timestamp' => $this->time,
            ],
        ];

        $this->assertCount(1, $client->postCalls);
        $this->assertEquals([true, true, false, false, false, false, false, false], $response);
        $this->assertEquals($expectedCalls, $client->postCalls);
    }

    public function testEmitTwoConnectionBatchesEarlyFailure()
    {
        $client = Yii::createObject([
            'class' => MockClient::class,
            'batchSize' => 2,
            'batchesPerPost' => 2,
        ]);

        $client->postResponse = [false, false]; // one 'true' for each batch

        $connection1Batch1 = [
            0 => ['a' => 1, 'b' => 2],
            1 => ['a' => 2, 'b' => 2],
        ];
        $connection1Batch2 = [
            2 => ['a' => 3, 'b' => 2],
            3 => ['a' => 4, 'b' => 2],
        ];
        $connection2Batch1 = [
            4 => ['a' => 1, 'b' => 4],
            5 => ['a' => 2, 'b' => 4],
        ];
        $connection2Batch2 = [
            6 => ['a' => 3, 'b' => 4],
            7 => ['a' => 4, 'b' => 4],
        ];
        $records = array_merge($connection1Batch1,
            $connection1Batch2,
            $connection2Batch1,
            $connection2Batch2);

        $response = $client->emitBatch('mytag', $records, $this->time);

        $expectedCalls = [
            [
                'tag' => 'mytag',
                'records' => ['[{"a":1,"b":2},{"a":2,"b":2}]','[{"a":3,"b":2},{"a":4,"b":2}]'],
                'timestamp' => $this->time,
            ],
        ];

        $this->assertCount(1, $client->postCalls);
        $this->assertEquals([false, false, false, false, false, false, false, false], $response);
        $this->assertEquals($expectedCalls, $client->postCalls);
    }

    public function testSerializeDefault()
    {
        $client = Yii::createObject([
            'class' => MockClient::class,
        ]);

        $record = ['a' => 1, 'b' => 2];
        $expected = '{"a":1,"b":2}';
        $this->assertEquals($expected, $client->serialize($record));
    }

    public function testSerializeMsgpack()
    {
        $client = Yii::createObject([
            'class' => MockClient::class,
            'serializer' => function($data) {
                return md5(json_encode($data));
            },
        ]);

        $record = ['a' => 1, 'b' => 2];
        $expected = '608de49a4600dbb5b173492759792e4a';
        $this->assertEquals($expected, $client->serialize($record));
    }
}

class MockClient extends \thamtech\fluentd\FireForgetHttpFluentClient
{
    public $postCalls = [];
    public $postResponse = [true];

    public function init()
    {
        $this->postCalls = [];
        $this->postResponse = [true];
    }

    protected function post($tag, $records, $timestamp = null)
    {
        $this->postCalls[] = [
            'tag' => $tag,
            'records' => $records,
            'timestamp' => $timestamp,
        ];
        if ($this->postResponse instanceof \Exception) {
            throw $this->postResponse;
        }
        return $this->postResponse;
    }

    public function serialize($data)
    {
        // increase visibility for testing
        return parent::serialize($data);
    }
}
