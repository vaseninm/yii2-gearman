<?php

namespace vaseninm\components;

use yii\base\Component;
use yii\base\Exception;
use yii\helpers\Json;

class GearmanComponent extends Component
{

    CONST PRIORITY_LOW = 'LOW';
    CONST PRIORITY_NORMAL = 'NORMAL';
    CONST PRIORITY_HIGH = 'HIGH';

    /**
     * @var array ['127.0.0.3:4321', '127.0.0.4:1234'], 127.0.0.1:4730 by default
     */
    public $servers = [];
    /**
     * @var array http://php.net/manual/ru/gearmanclient.addoptions.php for example [GEARMAN_CLIENT_GENERATE_UNIQUE, GEARMAN_CLIENT_NON_BLOCKING]
     */
    public $clientOptions = [];
    /**
     * @var array http://php.net/manual/en/gearmanworker.addoptions.php
     */
    public $workerOptions = [];

    /**
     * @var \GearmanClient
     */
    protected $client = null;

    /**
     * @var \GearmanWorker
     */
    protected $worker = null;

    public function init() {

        $this->client = new \GearmanClient();
        $this->worker = new \GearmanWorker();

        if (empty($this->servers)) {
            $this->servers = ['localhost'];
        }

        $this->client->addServers(implode(',', $this->servers));
        $this->client->setOptions(implode(' | ', $this->clientOptions));
        $this->worker->addServers(implode(',', $this->servers));
        $this->worker->setOptions(implode(' | ', $this->workerOptions));
    }

    /**
     * @param string $function function name
     * @param mixed $data data
     * @param bool $async
     * @param string $priority
     * @return mixed The job handle for the submitted task
     * @throws Exception
     */
    public function push($function, $data, $async = true, $priority = self::PRIORITY_NORMAL) {
        switch($priority) {
            case self::PRIORITY_LOW:
                $exec = $async ? 'doLowBackground' : 'doLow';
                break;
            case self::PRIORITY_NORMAL:
                $exec = $async ? 'doBackground' : 'doNormal';
                break;
            case self::PRIORITY_HIGH:
                $exec = $async ? 'doHighBackground' : 'doHigh';
                break;
            default:
                throw new Exception('Wrong Gearman priority');
        }

        $data = Json::encode([
            'data' => $data,
        ]);

        return $this->client->{$exec}($function, $data); //@todo uniqueID
    }

    /**
     * @param string $function function name
     * @param mixed $data data
     * @param string $priority
     * @return mixed The job handle for the submitted task
     * @throws Exception
     */
    public function asyncPush($function, $data, $priority = self::PRIORITY_NORMAL) {
        return $this->push($function, $data, true, $priority);
    }

    /**
     * @param string $function function name
     * @param mixed $data data
     * @param string $priority
     * @return mixed The job handle for the submitted task
     * @throws Exception
     */
    public function syncPush($function, $data, $priority = self::PRIORITY_NORMAL) {
        return $this->push($function, $data, false, $priority);
    }

    /**
     * @param string $function function name
     * @param callable $callback callable function function($data)
     * @return bool
     */
    public function register($function, callable $callback) {
        return $this->worker->addFunction($function, function (\GearmanJob $job) use ($callback) {
            $result = Json::decode($job->workload());
            return $callback($result['data']);
        });
    }

    /**
     * do it
     * @return bool
     */
    public function pull() {
        return $this->worker->work();
    }
}