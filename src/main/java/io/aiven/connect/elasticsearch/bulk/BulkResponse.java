/*
 * Copyright 2020 Aiven Oy
 * Copyright 2016 Confluent Inc.
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
 */

package io.aiven.connect.elasticsearch.bulk;

public class BulkResponse {

    private static final BulkResponse SUCCESS_RESPONSE = new BulkResponse(true, false, "");

    public final boolean succeeded;
    public final boolean retriable;
    public final String errorInfo;

    private BulkResponse(final boolean succeeded, final boolean retriable, final String errorInfo) {
        this.succeeded = succeeded;
        this.retriable = retriable;
        this.errorInfo = errorInfo;
    }

    public static BulkResponse success() {
        return SUCCESS_RESPONSE;
    }

    public static BulkResponse failure(final boolean retriable, final String errorInfo) {
        return new BulkResponse(false, retriable, errorInfo);
    }

    public boolean isSucceeded() {
        return succeeded;
    }

    public boolean isRetriable() {
        return retriable;
    }

    public String getErrorInfo() {
        return errorInfo;
    }

}
