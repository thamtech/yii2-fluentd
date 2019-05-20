<?php

namespace thamtechunit\fluentd;

use yii\log\LogRuntimeException;
use yii\log\Logger;
use Yii;

class FluentTargetTest extends \thamtechunit\fluentd\TestCase
{
    public $e;
    public $eLine;
    public $logRuntimeException;
    public $time;

    protected function setUp(): void
    {
        parent::setUp();
        $this->e = new \Exception('abc 123'); $this->eLine = __LINE__;
        $this->logRuntimeException = new LogRuntimeException('runtime abc 123');
        $this->time = time();
        $_GET['param1'] = 'value1';
    }

    public function testFormatMessageException()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
        ]);

        $expected = [
            'context' => [
                '_GET' => [
                    'param1' => 'value1',
                ],
            ],
            'timestamp' => $this->time,
            'level' => 'info',
            'category' => 'mycategory',
            'message' => [
                'exception' => (string) $this->e,
            ],
        ];

        $formatted = $target->formatMessage([$this->e, Logger::LEVEL_INFO, 'mycategory', $this->time]);
        $this->assertEquals($expected, $formatted);
    }

    public function testFormatMessageExceptionAsArray()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'formatExceptionAsArray' => true,
        ]);

        $expected = [
            'context' => [
                '_GET' => [
                    'param1' => 'value1',
                ],
            ],
            'timestamp' => $this->time,
            'level' => 'info',
            'category' => 'mycategory',
            'message' => [
                'exception' => [
                    'class' => 'Exception',
                    'message' => 'abc 123',
                    'file' => __FILE__,
                    'line' => $this->eLine,
                    'trace' => explode("\n", $this->e->getTraceAsString()),
                ],
            ],
        ];

        $formatted = $target->formatMessage([$this->e, Logger::LEVEL_INFO, 'mycategory', $this->time]);
        $this->assertEquals($expected, $formatted);
    }

    public function testFormatMessageArraySuffix()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'suffix' => [
                'abc' => 123,
                'def' => 456,
                'level' => new \yii\helpers\UnsetArrayValue(),
                'message' => [
                    'b' => new \yii\helpers\ReplaceArrayValue(['replaced' => 3]),
                ],
            ],
        ]);

        $expected = [
            'context' => [
                '_GET' => [
                    'param1' => 'value1',
                ],
            ],
            'timestamp' => $this->time,
            'category' => 'mycategory',
            'message' => [
                'a' => 1,
                'b' => [
                    'replaced' => 3,
                ],
            ],
            'abc' => 123,
            'def' => 456,
        ];

        $formatted = $target->formatMessage([['a' => 1, 'b' => ['c' => 2]], Logger::LEVEL_INFO, 'mycategory', $this->time]);
        $this->assertEquals($expected, $formatted);
    }

    public function testFormatMessageCallableSuffix()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'suffix' => function($message, $data) {
                $data['abc'] = 123;
                $data['def'] = 456;
                unset($data['level']);
                $data['message']['b'] = ['replaced' => 3];
                return $data;
            }
        ]);

        $expected = [
            'context' => [
                '_GET' => [
                    'param1' => 'value1',
                ],
            ],
            'timestamp' => $this->time,
            'category' => 'mycategory',
            'message' => [
                'a' => 1,
                'b' => [
                    'replaced' => 3,
                ],
            ],
            'abc' => 123,
            'def' => 456,
        ];

        $formatted = $target->formatMessage([['a' => 1, 'b' => ['c' => 2]], Logger::LEVEL_INFO, 'mycategory', $this->time]);
        $this->assertEquals($expected, $formatted);
    }

    public function testFormatMessageHideKeys()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'hideKeys' => ['timestamp', 'level'],
        ]);

        $expected = [
            'context' => [
                '_GET' => [
                    'param1' => 'value1',
                ],
            ],
            'category' => 'mycategory',
            'message' => [
                'a' => 1,
                'b' => [
                    'c' => 2,
                ],
            ],
        ];

        $formatted = $target->formatMessage([['a' => 1, 'b' => ['c' => 2]], Logger::LEVEL_INFO, 'mycategory', $this->time]);
        $this->assertEquals($expected, $formatted);
    }

    public function testFormatMessagePrefixDefault()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
        ]);

        $expected = [
            'context' => [
                '_GET' => [
                    'param1' => 'value1',
                ],
            ],
            'timestamp' => $this->time,
            'level' => 'info',
            'category' => 'mycategory',
            'message' => [
                'a' => 1,
                'b' => [
                    'c' => 2,
                ],
            ],
        ];

        $formatted = $target->formatMessage([['a' => 1, 'b' => ['c' => 2]], Logger::LEVEL_INFO, 'mycategory', $this->time]);
        $this->assertEquals($expected, $formatted);
    }

    public function testFormatMessagePrefixCallable()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'prefix' => function($message) {
                return [
                    'time' => $message[3],
                    'abc' => 123,
                    'def' => 456,
                ];
            }
        ]);

        $expected = [
            'time' => $this->time,
            'abc' => 123,
            'def' => 456,
            'timestamp' => $this->time,
            'level' => 'info',
            'category' => 'mycategory',
            'message' => [
                'a' => 1,
                'b' => [
                    'c' => 2,
                ],
            ],
        ];

        $formatted = $target->formatMessage([['a' => 1, 'b' => ['c' => 2]], Logger::LEVEL_INFO, 'mycategory', $this->time]);
        $this->assertEquals($expected, $formatted);
    }

    public function testFormatMessageMergeArrayMessage()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'mergeArrayMessage' => true,
        ]);

        $expected = [
            'context' => [
                '_GET' => [
                    'param1' => 'value1',
                ],
            ],
            'timestamp' => $this->time,
            'level' => 'info',
            'category' => 'mycategory',
            'a' => 1,
            'b' => [
                'c' => 2,
            ],
        ];

        $formatted = $target->formatMessage([['a' => 1, 'b' => ['c' => 2]], Logger::LEVEL_INFO, 'mycategory', $this->time]);
        $this->assertEquals($expected, $formatted);

        $expected = [
            'context' => [
                '_GET' => [
                    'param1' => 'value1',
                ],
            ],
            'timestamp' => $this->time,
            'level' => 'info',
            'category' => 'mycategory',
            'message' => 'mymessage',
        ];
        $formatted = $target->formatMessage(['mymessage', Logger::LEVEL_INFO, 'mycategory', $this->time]);
        $this->assertEquals($expected, $formatted);
    }

    public function testGetFilteredMessagesDefault()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
        ]);

        $messages = [
            [['a' => 1, 'b' => 2], Logger::LEVEL_INFO, 'mycategory', $this->time],
            [['a' => 2, 'b' => 2], Logger::LEVEL_INFO, 'mycategory', $this->time],
            [['a' => 3, 'b' => 3], Logger::LEVEL_INFO, 'mycategory', $this->time],
        ];

        $expected = [
            [
                'context' => [
                    '_GET' => [
                        'param1' => 'value1',
                    ],
                ],
                'timestamp' => $this->time,
                'level' => 'info',
                'category' => 'mycategory',
                'message' => ['a' => 1, 'b' => 2],
            ],
            [
                'context' => [
                    '_GET' => [
                        'param1' => 'value1',
                    ],
                ],
                'timestamp' => $this->time,
                'level' => 'info',
                'category' => 'mycategory',
                'message' => ['a' => 2, 'b' => 2],
            ],
            [
                'context' => [
                    '_GET' => [
                        'param1' => 'value1',
                    ],
                ],
                'timestamp' => $this->time,
                'level' => 'info',
                'category' => 'mycategory',
                'message' => ['a' => 3, 'b' => 3],
            ]
        ];

        $target->messages = $messages;
        $formatted = $target->getFormattedMessages();
        $this->assertEquals($expected, $formatted);
    }

    public function testGetFilteredMessagesFiltered()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'filter' => function($message) {
                return $message[0]['a'] > 1;
            }
        ]);

        $messages = [
            [['a' => 1, 'b' => 2], Logger::LEVEL_INFO, 'mycategory', $this->time],
            [['a' => 2, 'b' => 2], Logger::LEVEL_INFO, 'mycategory', $this->time],
            [['a' => 3, 'b' => 3], Logger::LEVEL_INFO, 'mycategory', $this->time],
        ];

        $expected = [
            [
                'context' => [
                    '_GET' => [
                        'param1' => 'value1',
                    ],
                ],
                'timestamp' => $this->time,
                'level' => 'info',
                'category' => 'mycategory',
                'message' => ['a' => 2, 'b' => 2],
            ],
            [
                'context' => [
                    '_GET' => [
                        'param1' => 'value1',
                    ],
                ],
                'timestamp' => $this->time,
                'level' => 'info',
                'category' => 'mycategory',
                'message' => ['a' => 3, 'b' => 3],
            ]
        ];

        $target->messages = $messages;
        $formatted = $target->getFormattedMessages();
        $this->assertEquals($expected, $formatted);
    }


    public function testExportSingleSuccess()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'tag' => 'mytag',
        ]);

        $target->messages = [['mymessage', Logger::LEVEL_INFO, 'mycategory', $this->time]];
        $target->export();

        $expectedCall = [
            'tag' => 'mytag',
            'records' => [[
                'context' => [
                    '_GET' => [
                        'param1' => 'value1',
                    ],
                ],
                'timestamp' => $this->time,
                'level' => 'info',
                'category' => 'mycategory',
                'message' => 'mymessage',
            ]],
            'timestamp' => null,
        ];

        $this->assertCount(0, $target->client->emitCalls);
        $this->assertCount(1, $target->client->emitBatchCalls);
        $this->assertEquals($expectedCall, $target->client->emitBatchCalls[0]);
    }

    public function testExportMultipleSuccess()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'tag' => 'mytag',
        ]);

        $target->messages = [
            ['mymessage', Logger::LEVEL_INFO, 'mycategory', $this->time],
            ['mymessage2', Logger::LEVEL_INFO, 'mycategory', $this->time],
        ];
        $target->export();

        $expectedCall = [
            'tag' => 'mytag',
            'records' => [
                [
                    'context' => [
                        '_GET' => [
                            'param1' => 'value1',
                        ],
                    ],
                    'timestamp' => $this->time,
                    'level' => 'info',
                    'category' => 'mycategory',
                    'message' => 'mymessage',
                ],
                [
                    'context' => [
                        '_GET' => [
                            'param1' => 'value1',
                        ],
                    ],
                    'timestamp' => $this->time,
                    'level' => 'info',
                    'category' => 'mycategory',
                    'message' => 'mymessage2',
                ],
            ],
            'timestamp' => null,
        ];

        $this->assertCount(0, $target->client->emitCalls);
        $this->assertCount(1, $target->client->emitBatchCalls);
        $this->assertEquals($expectedCall, $target->client->emitBatchCalls[0]);
    }

    public function testExportSingleFalseResult()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'tag' => 'mytag',
        ]);

        $target->client->emitBatchResponse = [false];

        $target->messages = [['mymessage', Logger::LEVEL_INFO, 'mycategory', $this->time]];

        $this->expectException(\yii\log\LogRuntimeException::class);
        $this->expectExceptionMessage('Unable to emit batch of log messages to Fluentd: 1 failed message(s) out of 1 total.');
        $target->export();
    }

    public function testExportSingleLogRuntimeException()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'tag' => 'mytag',
        ]);

        $target->client->emitBatchResponse = new \yii\log\LogRuntimeException('Test Runtime Exception');

        $target->messages = [['mymessage', Logger::LEVEL_INFO, 'mycategory', $this->time]];

        $this->expectException(\yii\log\LogRuntimeException::class);
        $this->expectExceptionMessage('Test Runtime Exception');
        $target->export();
    }


    public function testExportSingleException()
    {
        $target = Yii::createObject([
            'class' => MockFluentTarget::class,
            'client' => MockClient1::class,
            'tag' => 'mytag',
        ]);

        $target->client->emitBatchResponse = new \Exception('Test Exception');

        $target->messages = [['mymessage', Logger::LEVEL_INFO, 'mycategory', $this->time]];

        $this->expectException(\yii\log\LogRuntimeException::class);
        $this->expectExceptionMessage('Caught exception while attempting to export log messages to Fluentd: Test Exception');
        $target->export();
    }

}

class MockClient1 extends \yii\base\BaseObject implements \thamtech\fluentd\FluentClientInterface
{
    public $emitCalls = [];
    public $emitBatchCalls = [];
    public $emitResponse = true;
    public $emitBatchResponse = [true];

    public function init()
    {
        $this->emitCalls = [];
        $this->emitBatchCalls = [];
        $this->emitResponse = true;
        $this->emitBatchResponse = [true];
    }

    public function emit($tag, $record, $timestamp = null) {
        $this->emitCalls[] = [
            'tag' => $tag,
            'record' => $record,
            'timestamp' => $timestamp,
        ];
        if ($this->emitResponse instanceof \Exception) {
            throw $this->emitResponse;
        }
        return $this->emitResponse;
    }

    public function emitBatch($tag, array $records, $timestamp = null) {
        $this->emitBatchCalls[] = [
            'tag' => $tag,
            'records' => $records,
            'timestamp' => $timestamp,
        ];
        if ($this->emitBatchResponse instanceof \Exception) {
            throw $this->emitBatchResponse;
        }
        return $this->emitBatchResponse;
    }
}

class MockFluentTarget extends \thamtech\fluentd\FluentTarget
{
    public $logVars = ['_GET'];

    // increase visibility for testing
    public function getFormattedMessages()
    {
        return parent::getFormattedMessages();
    }
}
