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

/**
 * FireForgetHttpFluentClient emits records to a Fluentd HTTP input plugin
 * without regard to the HTTP response. The result is significantly faster
 * emissions.
 *
 * @author Tyler Ham <tyler@thamtech.com>
 */
class FireForgetHttpFluentClient extends BaseObject implements FluentClientInterface
{
    /**
     * @var string the HTTP host of the Fluentd service
     */
    public $host = '127.0.0.1';

    /**
     * @var int the port number of the Fluentd HTTP service
     */
    public $port = 9880;

    /**
     * @var int default timeout when trying to initiate each HTTP connection
     */
    public $connectionTimeout = 30;

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
     * @var int The maximum number of records to encode in a single HTTP post.
     */
    public $batchSize = 200;

    /**
     * @var int The maximum number of batches to send during a single socket
     * connection.
     */
    public $batchesPerConnection = 10;

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
        $connectionBatches = [];
        $connectionBatchesKeys = [];
        $r = 0;
        $failureOccurred = false;

        while ($r < count($records)) {
            // capture the first batch and add it to the array of connectionBatches
            $batch = array_slice($records, $r, $this->batchSize, true);
            $batchKeys = array_keys($batch);
            $connectionBatches[] = $batch;
            $connectionBatchesKeys[] = $batchKeys;
            $r+= count($batch);

            // if we've gathered enough batches for one connection (or we've
            // reached the end of the list of records):
            if (count($connectionBatches) >= $this->batchesPerConnection || (count($connectionBatches) > 0 && $r >= count($records)-1)) {
                $connectionBatchesResults = $this->post($tag, $connectionBatches, $timestamp);

                // capture the results and make a note of any failures
                foreach ($connectionBatchesResults as $i=>$result) {
                    $connectionBatchKeys = $connectionBatchesKeys[$i];
                    $connectionBatchResults = array_fill(0, count($connectionBatchKeys), $result);
                    $results+= array_combine($connectionBatchKeys, $connectionBatchResults);

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

                // empty the array of connectionBatches so we can start filling
                // up a new set for another connection
                $connectionBatches = [];
                $connectionBatchesKeys = [];
            }
        }

        return $results;
    }

    /**
     * Open a socket connection and send an HTTP POST for each record
     *
     * @param  string $tag The fluentd tag
     * @param  array $records array of one or more records or recordsets to post
     * @param  float|int $timestamp The time in seconds
     *
     * @return bool[] an array indicating the success or failure of each record
     */
    protected function post($tag, array $records, $timestamp = null)
    {
        $fp = fsockopen($this->host, $this->port, $errno, $errstr, $this->connectionTimeout);
        if (!$fp) {
            throw new \Exception('Unable to open a connection to ' . $this->host . ':' . $this->port . ', ' . $errstr, $errno);
        }

        $results = [];
        $result = true;

        $i = 0;
        foreach ($records as $k=>$data) {
            // stop attempting to emit records if we had one fail
            if (!$result) {
                // populate the rest of the result indicators as false
                $results[$k] = false;
                continue;
            }
            $output = "POST /" . $tag . ($timestamp ? '?time=' . $timestamp : '') . " HTTP/1.1\r\n";
            $output.= "Host: " . $this->host . "\r\n";
            $output.= "Content-Type: " . $this->contentType . "\r\n";

            $serialized = $this->serialize($data);

            $output.= "Content-Length: " . strlen($encoded) . "\r\n";
            $output.= "Connection: " . ($i == (count($records) - 1) ? "Close" : "Keep-Alive") . "\r\n\r\n";
            $output.= $serialized;

            $bytes = fwrite($fp, $output);
            if ($bytes == strlen($output)) {
                $results[$k] = true;
            } else {
                $results[$k] = false;
                $result = false;
            }
        }

        // if we don't read to the end of the responses, Fluentd may stop
        // processing the records part way through and leave some of them
        // unsaved.
        while (!feof($fp)) {
            fread($fp, 1024);
        }
        fclose($fp);

        return $results;
    }

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
