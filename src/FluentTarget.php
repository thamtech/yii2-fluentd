<?php
/**
 * @copyright Copyright(c) 2019 Thamtech, LLC
 * @link https://github.com/thamtech/yii2-fluentd
 * @license https://opensource.org/licenses/BSD-3-Clause
**/

namespace thamtech\fluentd;

use yii\di\Instance;
use yii\helpers\ArrayHelper;
use yii\log\Logger;
use yii\log\LogRuntimeException;
use yii\log\Target;
use yii\web\Request;
use Yii;

/**
 * FluentTarget sends log messages to a TCP, UDP, or socket-based Fluentd
 * service.
 *
 * @author Tyler Ham <tyler@thamtech.com>
 */
class FluentTarget extends Target
{
    /**
     * @var string tag to associate with messages sent to the Fluentd server
     * through this target.
     */
    public $tag;

    /**
     * @var bool indicates whether message text should be merged with the JSON data
     * to be logged if it is an array. Otherwise (by default), the message text
     * is set into the `message` key of the JSON data being logged.
     */
    public $mergeArrayMessage = false;

    /**
     * @var FluentClientInterface A FluentClientInterface, the name of a
     * FluentClientInterface application component or singleton, or a
     * configuration array.
     */
    public $client = [];

    /**
     * @var bool indicates whether to format an exception message as an array
     * or as the `__toString` serializtion of the exception.
     */
    public $formatExceptionAsArray = false;

    /**
     * @var array|callable an array or PHP callable.
     *
     * If an array, it will be merged with every formatted message. You can use
     * this to append/overwrite keys in every formatted message, or you can use
     * the \yii\helpers\UnsetArrayValue to remove keys that would otherwise
     * be included.
     *
     * A callable should accept the raw message array as the first parameter and
     * the formatted message as the second parameter and return the formatted
     * message array after making any desired changes.
     */
    public $suffix = [];

    /**
     * @var callable a PHP callable to determine if a message should be logged.
     * The method should take a single parameter, $message, and return true
     * if it should be logged, and false if it should be skipped.
     */
    public $filter;

    /**
     * @var array a set of keys associated with \yii\helpers\UnsetArrayValue to be
     * merged with every formatted message. See [[setHideKeys]].
     */
    private $_hideKeys = [];

    /**
     * {@inheritdoc}
     */
    public function init()
    {
        parent::init();

        // set default class
        if (is_array($this->client)) {
            $this->client = ArrayHelper::merge([
                'class' => 'thamtech\fluentd\FireForgetHttpFluentClient',
            ]);
        }
        $this->client = Instance::ensure($this->client, 'thamtech\fluentd\FluentClientInterface');
    }

    /**
     * Writes log messages to Fluentd.
     * @throws LogRuntimeException
     */
    public function export()
    {
        try {
            $messages = $this->getFormattedMessages();
            $results = $this->client->emitBatch($this->tag, $messages);
            $failures = 0;
            foreach ($results as $result) {
                if (!$result) {
                    ++$failures;
                }
            }

            if ($failures) {
                throw new LogRuntimeException('Unable to emit batch of log messages to Fluentd: ' . $failures . ' failed message(s) out of ' . count($messages) . ' total.');
            }
        } catch (LogRuntimeException $e) {
            // pass through
            throw $e;
        } catch (\Exception $e) {
            // convert to LogRuntimeException
            throw new LogRuntimeException('Caught exception while attempting to export log messages to Fluentd: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function formatMessage($message)
    {
        list($text, $level, $category, $timestamp) = $message;
        $level = Logger::getLevelName($level);
        if (!is_string($text)) {
            // exceptions may not be serializable if in the call stack somewhere is a Closure
            if ($text instanceof \Throwable || $text instanceof \Exception) {
                if ($this->formatExceptionAsArray) {
                    $text = [
                        'exception' => [
                            'class' => get_class($text),
                            'message' => $text->getMessage(),
                            'file' => $text->getFile(),
                            'line' => $text->getLine(),
                            'trace' => explode("\n", $text->getTraceAsString()),
                        ],
                    ];
                } else {
                    $text = ['exception' => (string) $text];
                }
            }
        }

        $prefix = $this->getMessagePrefix($message);

        $data = ArrayHelper::merge($prefix, [
            'timestamp' => $timestamp,
            'level' => $level,
            'category' => $category,
        ]);

        if (!is_array($text) || !$this->mergeArrayMessage) {
            $text = ['message' => $text];
        }

        $data = ArrayHelper::merge($data, $text);

        if (is_callable($this->suffix)) {
            $data = call_user_func($this->suffix, $message, $data);
        } else {
            $data = ArrayHelper::merge($data, $this->suffix);
        }

        return ArrayHelper::merge($data, $this->_hideKeys);
    }

    /**
     * {@inheritdoc}
     */
    public function getMessagePrefix($message)
    {
        if ($this->prefix !== null) {
            return call_user_func($this->prefix, $message);
        }

        $prefix = [];

        if (Yii::$app !== null) {
            $request = Yii::$app->getRequest();
            if ($request instanceof Request) {
                $prefix['ip'] = $request->getUserIP();
            }

            /* @var $user \yii\web\User */
            $user = Yii::$app->has('user', true) ? Yii::$app->get('user') : null;
            if ($user && ($identity = $user->getIdentity(false))) {
                $prefix['user_id'] = $identity->getId();
            }

            /* @var $session \yii\web\Session */
            $session = Yii::$app->has('session', true) ? Yii::$app->get('session') : null;
            if ($session && $session->getIsActive()) {
                $prefix['session_id'] = $session->getId();
            }
        }

        $prefix['context'] = ArrayHelper::filter($GLOBALS, $this->logVars);

        return $prefix;
    }

    /**
     * Mark a set of keys that should be removed from each record before emitting it.
     * These keys will be removed after any [[suffix]] array is merged or callback is
     * executed.
     *
     * @param string[] $keys An array of keys to hide.
     */
    public function setHideKeys(array $keys)
    {
        $this->_hideKeys = [];
        foreach ($keys as $key) {
            $this->_hideKeys[$key] = new \yii\helpers\UnsetArrayValue();
        }
    }

    /**
     * Get formatted messages filtered based on the [[filter]] callable, if any.
     *
     * @return array array of formatted messages that pass the filter condition, if any
     */
    protected function getFormattedMessages()
    {
        $formattedMessages = [];
        foreach ($this->messages as $message) {
            if (is_callable($this->filter) && !call_user_func($this->filter, $message)) {
                // The filter method determined that we shouldn't bother
                // logging this message
                continue;
            }

            $formattedMessages[] = $this->formatMessage($message);
        }
        return $formattedMessages;
    }
}
