/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.common.utils;

import org.apache.seatunnel.common.Constants;

import org.apache.commons.lang3.text.StrSubstitutor;

import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class VariablesSubstitute {

    private VariablesSubstitute() {}

    /**
     * @param text raw string
     * @param timeFormat example : "yyyy-MM-dd HH:mm:ss"
     * @return replaced text
     */
    public static String substitute(String text, String timeFormat) {
        DateTimeFormatter df = DateTimeFormatter.ofPattern(timeFormat);
        final String formattedDate = df.format(ZonedDateTime.now());

        final Map<String, String> valuesMap = new HashMap<>(3);
        valuesMap.put(Constants.UUID, UUID.randomUUID().toString());
        valuesMap.put(Constants.NOW, formattedDate);
        valuesMap.put(timeFormat, formattedDate);
        return substitute(text, valuesMap);
    }

    /**
     * @param text raw string
     * @param valuesMap key is variable name, value is substituted string.
     * @return replaced text
     */
    public static String substitute(String text, Map<String, String> valuesMap) {
        final StrSubstitutor sub = new StrSubstitutor(valuesMap);
        return sub.replace(text);
    }

    /**
     * @param text the text need to replace date variable to today with the format string
     * @return replaced text
     */
    public static String replaceDateVariableToToday(String text) {
        Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}");
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(matcher.group(1));
                Calendar calendar = Calendar.getInstance();
                String currentDateTime = sdf.format(calendar.getTime());
                text = text.replace(matcher.group(0), currentDateTime);
            } catch (Exception e) {
                throw new RuntimeException(
                        "The date format is illegal, please check the date variable in the path: "
                                + matcher.group(0),
                        e);
            }
        }
        return text;
    }
}
