/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.flume.handlers.debug;

import java.io.IOException;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This decorator adds a delay to the append operation. This will hopefully
 * allow for us to inject faults into messages being passed through the system.
 */
public class DelayDecorator<S extends EventSink> extends EventSinkDecorator<S> {

  final int millis;

  public DelayDecorator(S s, int millis) {
    super(s);
    this.millis = millis;
  }

  @Override
  public void append(Event e) throws IOException {
    try {
      Thread.sleep(millis);
      super.append(e);
    } catch (InterruptedException e1) {
      // TODO (jon) clean this up.
      throw new IOException("", e1);
    }

  }

  public static SinkDecoBuilder builder() {

    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions
            .checkArgument(argv.length <= 2, "usage: delay(init=1000)");
        int delaymillis = 1000;
        if (argv.length >= 1)
          delaymillis = Integer.parseInt(argv[0]);
        return new DelayDecorator<EventSink>(null, delaymillis);
      }

    };
  }

}