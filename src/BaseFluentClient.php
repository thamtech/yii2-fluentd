<?php
/*
 * Copyright (C) 2019 Thamtech, LLC
 *
 * This software is copyrighted. No part of this work may be
 * reproduced in whole or in part in any manner without the
 * permission of the Copyright owner, unless specifically authorized
 * by a license obtained from the Copyright owner.
**/

namespace thamtech\fluentd;

use yii\base\BaseObject;
use yii\helpers\Json;

abstract class BaseFluentClient implements FluentClientInterface
{
    /**
     * @var null|callable the function used to serialize a record or a batch of
     * records. Defaults to null, meaning the default Json::encode() method.
     * You could configure this as a callback, such as 'msgpack_pack'. If you
     * do, you may need to update [[contentType]] as well.
     */
    public $serializer;

    /**
     * @var string the content type that will be produced by the [[serializer]].
     */
    public $contentType = 'application/json';

    /**
     * @var int The maximum number of records to encode in a single Fluentd
     * batch record.
     */
    public $batchSize = 200;

    /**
     * @var int The maximum number of Fluentd batch records to send during a
     * single call to the post() method.
     */
    public $batchesPerPost = 10;

    /**
     * {@inheritdoc}
     */
    public function emit($tag, $record, $timestamp = null)
    {
        $results = $this->post($tag, [$record], $timestamp);
        return $results[0];
    }

    /**
     * {@inheritdoc}
     */
    public function emitBatch($tag, array $records, $timestamp = null)
    {
        $recordKeys = array_keys($records);
        $results = [];

        // array of batches
        //   - each batch will be encoded and sent in a separate HTTP POST
        //   - the whole array of batches will be sent through a single TCP connection/session
        $postBatches = [];
        $postBatchesKeys = [];
        $r = 0;
        $failureOccurred = false;

        while ($r < count($records)) {
            // capture the first batch and add it to the array of postBatches
            $batch = array_slice($records, $r, $this->batchSize, true);
            $batchKeys = array_keys($batch);
            $postBatches[] = $batch;
            $postBatchesKeys[] = $batchKeys;
            $r+= count($batch);

            // if we've gathered enough batches for one post (or we've
            // reached the end of the list of records):
            if (count($postBatches) >= $this->batchesPerPost || (count($postBatches) > 0 && $r >= count($records)-1)) {
                $postBatchResults = $this->post($tag, $postBatches, $timestamp);

                // capture the results and make a note of any failures
                foreach ($postBatchResults as $i=>$result) {
                    $postBatchkeys = $postBatchesKeys[$i];
                    $postBatchResults = array_fill(0, count($postBatchkeys), $result);
                    $results+= array_combine($postBatchkeys, $postBatchResults);

                    if (!$result) {
                        $failureOccurred = true;
                    }
                }

                // stop attempting to emit records if we had one fail
                if ($failureOccurred) {
                    // populate the rest of the results with false values and return
                    $remainingRecords = array_slice($records, $r, null, true);
                    $remainingKeys = array_keys($remainingRecords);
                    $falses = array_fill(0, count($remainingKeys), false);
                    $results+= array_combine($remainingKeys, $falses);
                    return $results;
                }

                // empty the array of postBatches so we can start filling
                // up a new set for another post
                $postBatches = [];
                $postBatchesKeys = [];
            }
        }

        return $results;
    }

    /**
     * Post a set of records to Fluentd. Each record in the given array should
     * be either a single record or a Fluentd batch array of records.
     *
     * @param  string $tag The fluentd tag
     * @param  array $records array of one or more records or recordsets to post
     * @param  float|int $timestamp The time in seconds
     *
     * @return bool[] an array indicating the success or failure of each item
     * in the $records array.
     */
    abstract protected function post($tag, array $records, $timestamp = null);

    /**
     * Serialize the data using the configured serializer. If no serializer
     * is configured, the default [[Json::encode()]] helper method will be
     * used.
     *
     * @param  mixed $data the data to be serialized
     *
     * @return string the serialized data
     */
    protected function serialize($data)
    {
        if ($this->serializer === null) {
            return Json::encode($data);
        }

        return call_user_func($this->serializer, $data);
    }
}
