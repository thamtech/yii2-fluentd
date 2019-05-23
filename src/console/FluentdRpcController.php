<?php
/**
 * @copyright Copyright(c) 2019 Thamtech, LLC
 * @link https://github.com/thamtech/yii2-fluentd
 * @license https://opensource.org/licenses/BSD-3-Clause
**/

namespace thamtech\fluentd\console;

use yii\console\Controller;
use yii\console\ExitCode;
use yii\helpers\Console;

/**
 * Allows you to manage a Fluentd instance via RPC commands.
 *
 * See: https://docs.fluentd.org/deployment/rpc
 *
 * @author Tyler Ham <tyler@thamtech.com>
 */
class FluentdRpcController extends Controller
{
    /**
     * @var string HTTP endpoint of the Fluentd RPC host
     */
    public $endpoint = 'http://127.0.0.1:24444';

    /**
     * {@inheritdoc}
     */
    public function options($actionId)
    {
        return array_merge(parent::options($actionId), [
            'endpoint',
        ]);
    }

    /**
     * Flush the fluentd buffers.
     *
     * This is just a shorcut for calling `invoke plugins.flushBuffers`.
     */
    public function actionFlush()
    {
        return $this->actionInvoke('plugins.flushBuffers');
    }

    /**
     * Reload the fluentd configuration.
     *
     * This is just a shortcut for calling `invoke config.reload`.
     */
    public function actionReloadConf()
    {
        return $this->actionInvoke('config.reload');
    }

    /**
     * Invoke the specified API.
     *
     * @param  string $api The API to invoke:
     *     - processes.interruptWorkers
     *     - processes.killWorkers
     *     - processes.flushBuffersAndKillWorkers
     *     - plugins.flushBuffers
     *     - config.reload
     */
    public function actionInvoke($api)
    {
        $url = rtrim($this->endpoint, ')') . '/api/' . ltrim($api, '/');
        $response = file_get_contents($url);

        if ($response === false && empty($http_response_header)) {
            $this->stderr("\nUnable to connect to the endpoint.\n", Console::FG_RED);
            $this->stderr("Endpoint: ");
            $this->stderr($this->endpoint . "\n", Console::BOLD);
            $this->stderr("URL: ");
            $this->stderr($url . "\n\n", Console::BOLD);
            return ExitCode::UNSPECIFIED_ERROR;
        }

        if (strpos($http_response_header[0], '200 OK') == false) {
            $this->stderr($http_response_header[0] . "\n", Console::FG_RED);
            $this->stderr(join("\n", array_slice($http_response_header, 1)) . "\n");
            $this->stderr($response . "\n\n");
            print_r($response);
            return ExitCode::UNSPECIFIED_ERROR;
        }

        $this->stdout($http_response_header[0] . "\n", Console::FG_GREEN);
        $this->stdout(join("\n", array_slice($http_response_header, 1)) . "\n", Console::FG_GREY);
        $this->stdout($response . "\n\n", Console::BOLD);
        return ExitCode::OK;
    }
}
