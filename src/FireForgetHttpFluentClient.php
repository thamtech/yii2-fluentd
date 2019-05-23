<?php
/**
 * @copyright Copyright(c) 2019 Thamtech, LLC
 * @link https://github.com/thamtech/yii2-fluentd
 * @license https://opensource.org/licenses/BSD-3-Clause
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
    protected function post($tag, array $serializedRecords, $timestamp = null)
    {
        $fp = fsockopen($this->host, $this->port, $errno, $errstr, $this->connectionTimeout);
        if (!$fp) {
            throw new \Exception('Unable to open a connection to ' . $this->host . ':' . $this->port . ', ' . $errstr, $errno);
        }

        $results = [];
        $result = true;

        // Track our own index to loop through the records.
        // We are only using it to know when we're on the last record (
        // whether to send 'Close' or 'Keep-Alive' in the Connection
        // HTTP header), and we don't want to assume that the keys of
        // $serializedRecords are indexed and starting with 0.
        $numRecordsPosted = 0;

        foreach ($serializedRecords as $k=>$data) {
            // stop attempting to emit records if we had one fail
            if (!$result) {
                // populate the rest of the result indicators as false
                $results[$k] = false;
                continue;
            }

            $queryString = $timestamp
                ? '?time=' . $timestamp
                : '';

            // Keep-Alive unless we are on the last record
            $connection = ($numRecordsPosted == (count($serializedRecords) - 1))
                ? 'Close'
                : 'Keep-Alive';

            $output = "POST /" . $tag . $queryString . " HTTP/1.1\r\n";
            $output.= "Host: " . $this->host . "\r\n";
            $output.= "Content-Type: " . $this->contentType . "\r\n";
            $output.= "Content-Length: " . strlen($data) . "\r\n";
            $output.= "Connection: " . $connection . "\r\n\r\n";
            $output.= $data;

            $bytes = $this->writeStream($fp, $output);
            if ($bytes == strlen($output)) {
                $results[$k] = true;
            } else {
                $results[$k] = false;
                $result = false;
            }
            ++$numRecordsPosted;
        }

        // if we don't read to the end of the responses, Fluentd may stop
        // processing the records part way through and leave some of them
        // unsaved.
        $expectedBytesPerResponse = 88;
        $length = 8192 * ceil($numRecordsPosted * $expectedBytesPerResponse * 1.0 / 8192);
        while (!feof($fp)) {
            fread($fp, $length);
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
