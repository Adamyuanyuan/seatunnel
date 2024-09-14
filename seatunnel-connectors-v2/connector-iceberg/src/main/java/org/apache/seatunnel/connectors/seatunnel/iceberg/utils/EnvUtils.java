/*
 * Copyright 2023 Ververica Inc.
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
 */

package org.apache.seatunnel.connectors.seatunnel.iceberg.utils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

/** Supports {@link EnvUtils} to schema evolution. */
public class EnvUtils {
    public static void setEnv(String name, String value) {
        try {
            getModifiableEnvironment().put(name, value);
        } catch (Exception e) {
            throw new RuntimeException("set env Exception:", e);
        }
    }

    private static Map<String, String> getModifiableEnvironment() throws Exception {
        Class<?> pe = Class.forName("java.lang.ProcessEnvironment");
        Method getenv = pe.getDeclaredMethod("getenv");
        getenv.setAccessible(true);
        Object unmodifiableEnvironment = getenv.invoke(null);
        Class<?> map = Class.forName("java.util.Collections$UnmodifiableMap");
        Field m = map.getDeclaredField("m");
        m.setAccessible(true);
        return (Map<String, String>) m.get(unmodifiableEnvironment);
    }
}
