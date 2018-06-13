<?php

namespace Grpc;

require_once(dirname(__FILE__).'/generated/Grpc_gcp/ExtensionConfig.php');
require_once(dirname(__FILE__).'/generated/Grpc_gcp/AffinityConfig.php');
require_once(dirname(__FILE__).'/generated/Grpc_gcp/AffinityConfig_Command.php');
require_once(dirname(__FILE__).'/generated/Grpc_gcp/ApiConfig.php');
require_once(dirname(__FILE__).'/generated/Grpc_gcp/ChannelPoolConfig.php');
require_once(dirname(__FILE__).'/generated/Grpc_gcp/MethodConfig.php');
require_once(dirname(__FILE__).'/generated/GPBMetadata/GrpcGcp.php');


use Google\Auth\ApplicationDefaultCredentials;

class GCPCallInterceptor extends \Grpc\Interceptor
{
    public function interceptUnaryUnary($method,
                                        $argument,
                                        array $metadata = [],
                                        array $options = [],
                                        $continuation)
    {
        $call = new GCPUnaryCall(
            $continuation($method, $argument, $metadata, $options),
            $argument, $metadata);
        $call->start();
        return $call;
    }

    public function interceptUnaryStream($method,
                                         $argument,
                                         array $metadata = [],
                                         array $options = [],
                                         $continuation
    ) {
        $call = new GCPServerStreamCall(
            $continuation($method, $argument, $metadata, $options),
            $argument, $metadata);
        $call->start();
        return $call;
    }
}

class _ChannelRef
{
    private $opts;
    private $channel_id;
    private $affinity_ref;
    private $active_stream_ref;
    private $target;

    public function __construct($target, $channel_id, $opts, $affinity_ref=0, $active_stream_ref=0)
    {
        $this->target = $target;
        $this->channel_id = $channel_id;
        $this->affinity_ref = $affinity_ref;
        $this->active_stream_ref = $active_stream_ref;
        $this->opts = $opts;
    }

    public function getRealChannel($credentials) {

        // 'credentials' in the array $opts will be unset during creating the channel.
        if(!array_key_exists('credentials', $this->opts)){
            $this->opts['credentials'] = $credentials;
        }
        $real_channel = new \Grpc\Channel($this->target, $this->opts);
        return $real_channel;
    }

    public function getAffinityRef() {return $this->affinity_ref;}
    public function getActiveStreamRef() {return $this->active_stream_ref;}
    public function affinityRefIncr() {$this->affinity_ref += 1;}
    public function affinityRefDecr() {$this->affinity_ref -= 1;}
    public function activeStreamRefIncr() {$this->active_stream_ref += 1;}
    public function activeStreamRefDecr() {$this->active_stream_ref -= 1;}
}


class GCPUnaryCall extends GcpBaseCall
{
    private function createRealCall($channel) {
        $this->real_call = new UnaryCall($channel, $this->method, $this->deserialize, $this->options);
        return $this->real_call;
    }

    public function start() {
        $channel_ref = $this->rpcPreProcess($this->argument);
        $real_channel = $channel_ref->getRealChannel($this->gcp_channel->credentials);
        $this->real_call = $this->createRealCall($real_channel);
        $this->real_call->start($this->argument, $this->metadata, $this->options);
    }

    public function wait() {
        list($response, $status) = $this->real_call->wait();
        $this->rpcPostProcess($status, $response);
        return [$response, $status];
    }

    public function getMetadata() {
        return $this->real_call->getMetadata();
    }
}

class GCPServerStreamCall extends GcpBaseCall
{
    private $response = null;

    private function createRealCall($channel) {
        $this->real_call = new ServerStreamingCall($channel, $this->method, $this->deserialize, $this->options);
        return $this->real_call;
    }

    public function start() {
        $channel_ref = $this->rpcPreProcess($this->argument);
        $this->real_call = $this->createRealCall($channel_ref->getRealChannel(
            $this->gcp_channel->credentials));
        $this->real_call->start($this->argument, $this->metadata, $this->options);
    }

    public function responses() {
        $response = $this->real_call->responses();
        if($response) {
            $this->response = $response;
        }
        return $response;
    }

    public function getStatus() {
        $status = $this->real_call->getStatus();
        $this->rpcPostProcess($status, $this->response);
        return $status;
    }
}

abstract class GcpBaseCall
{
    protected $gcp_channel;
    protected $channel_ref;
    protected $affinity_key;
    protected $_affinity;

    protected $method;
    protected $argument;
    protected $metadata;
    protected $options;

    // Get all information needed to create a Call object and start the Call.
    public function __construct($empty_unary_unary_call,
                                $argument,
                                $metadata) {
        $empty_call = $empty_unary_unary_call->_getCall();
        $this->gcp_channel = $empty_call->_getChannel();
        $this->method = $empty_call->_getMethod();
        $this->deserialize = $empty_call->_getDeserialize();
        $this->options = $empty_call->_getOptions();
        $this->metadata = $empty_call->_getMetadata();
        $this->argument = $argument;
        $this->_affinity = $GLOBALS['global_conf']['affinity_by_method'][$this->method];
    }

    protected function rpcPreProcess($argument) {
        $this->affinity_key = null;
        if($this->_affinity) {
            $command = $this->_affinity['command'];
            if ($command == 'BOUND' || $command == 'UNBIND') {
                $this->affinity_key = $this->getAffinityKeyFromProto($argument);
            }
        }
        $this->channel_ref = $this->gcp_channel->getChannelRef($this->affinity_key);
        $this->channel_ref->activeStreamRefIncr();
        return $this->channel_ref;
    }

    protected function rpcPostProcess($status, $response) {
//    $gcp_channel = $global_conf['gcp_channel'.getmypid()];
        if($this->_affinity) {
            $command = $this->_affinity['command'];
            if ($command == 'BIND') {
                if ($status->code != \Grpc\STATUS_OK) {
                    return;
                }
                $affinity_key = $this->getAffinityKeyFromProto($response);
                $this->gcp_channel->_bind($this->channel_ref, $affinity_key);
            } else if ($command == 'UNBIND') {
                $this->gcp_channel->_unbind($this->affinity_key);
            }
//      $global_conf['gcp_channel' . getmypid()] = $gcp_channel;
        }
        $this->channel_ref->activeStreamRefDecr();
    }

    protected function getAffinityKeyFromProto($proto) {
        if($this->_affinity) {
            $names = $this->_affinity['affinityKey'];
            // TODO: names.split('.')
            $getAttrMethod = 'get'.ucfirst($names);
            $affinity_key = call_user_func_array(array($proto, $getAttrMethod), array());
            echo "[getAffinityKeyFromProto] $affinity_key\n";
            return $affinity_key;
        }
    }
}
