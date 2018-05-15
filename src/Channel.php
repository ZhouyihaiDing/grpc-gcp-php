<?php
/*
 *
 * Copyright 2018 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

namespace Grpc\Extensions;

class _ChannelRef
{
  private $_channel;
  private $_channel_id;
  private $_affinity_ref;
  private $_active_stream_ref;

  public function __construct($channel,
                              $channel_id,
                              $affinity_ref=0,
                              $active_stream_ref=0)
  {
        $this->_channel = $channel;
        $this->_channel_id = $channel_id;
        $this->_affinity_ref = $affinity_ref;
        $this->_active_stream_ref = $active_stream_ref;
  }

  public function affinity_ref_incr()
  {
    $this->_affinity_ref += 1;
  }

  public function affinity_ref_decr
  {
    $this->_affinity_ref -= 1;
  }

  public function affinity_ref()
  {
    return $this->_affinity_ref;
  }

  public function active_stream_ref_incr()
  {
    $this->_active_stream_ref += 1;
  }

  public function active_stream_ref_decr()
  {
    $this->_active_stream_ref -= 1;
  }

  public function active_stream_ref()
  {
    return $this->_active_stream_ref;
  }

  public function channel()
  {
    return $this->_channel;
  }
}

/**
 *
 */
class Channel
{

  public function __construct($target,
                              $options = [],
                              $credentials = NULL)
  {
  }

}
