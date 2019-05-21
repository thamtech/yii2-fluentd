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

/**
 * FireForgetHttpFluentClient emits records to a Fluentd HTTP input plugin
 * without regard to the HTTP response. The result is significantly faster
 * emissions.
 *
 * @author Tyler Ham <tyler@thamtech.com>
 */
class FireForgetHttpFluentClient extends BaseFluentClient
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
     * @var int maximum number of consecutive failed writes over the network
     * before giving up.
     */
    public $writeRetries = 3;

    /**
     * @var int microseconds delay between each write retry
     */
    public $writeRetryDelay = 1000;

    /**
     * Open a socket connection and send an HTTP POST for each record
     *
     * {@inheritdoc}
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

            $bytes = $this->writeStream($fp, $output);
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
     * A wrapper around `fwrite` that will loop until all bytes have been
     * written, or until the `fwrite` call fails a consecutive number of times
     * specified by [[writeRetries]].
     *
     * @param  resource $fp file system pointer resource
     *
     * @param  string $string the string to write
     *
     * @return int the number of bytes written
     */
    protected function writeStream($fp, $string)
    {
        $retriesRemaining = $this->writeRetries;
        $written = 0;
        while ($written < strlen($string) && $retriesRemaining) {
            $result = ($retriesRemaining > 1)
                ? @fwrite($fp, substr($string, $written))
                : fwrite($fp, substr($string, $written));

            if (!$result) {
                --$retriesRemaining;
                if ($retriesRemaining) {
                    usleep($this->writeRetryDelay);
                    continue;
                } else {
                    error_log('fwrite in ' . __FILE__ . ' failed after writing ' . $written . ' bytes out of ' . strlen($string));
                    return $written;
                }
            }
            // reset retries as long as we're making progress
            $retriesRemaining = $this->writeRetries;

            $written+= $result;
        }

        return $written;
    }
}
