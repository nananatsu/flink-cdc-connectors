/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common.meta;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Copied from https://github.com/tikv/client-java project. This class is mapping TiDB's CIStr/ For
 * internal use only.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class CIStr {
    private final String o; // original
    private final String l;

    @JsonCreator
    private CIStr(@JsonProperty("O") String o, @JsonProperty("L") String l) {
        this.o = o;
        this.l = l;
    }

    public static CIStr newCIStr(String str) {
        return new CIStr(str, str.toLowerCase());
    }

    public String getO() {
        return o;
    }

    public String getL() {
        return l;
    }
}
