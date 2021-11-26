package com.github.castorm.kafka.connect.util;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 - 2021 Cástor Rodríguez
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Alex.Sun
 * @created 2021-11-24 10:14
 */
public class ExitUtils {

    public static final String MSG_DONE = "[此异常为正常退出，请忽略]";

    private static final AtomicBoolean exiting = new AtomicBoolean(false);

    public static void forceExit(int code, String msg) {
        if (!exiting.get()) {
            exiting.set(true);

            new Thread(() -> {
                Utils.sleep(60000);
                Exit.halt(code, msg);
            }).start();

            Exit.exit(code, msg);
        }
    }
}
