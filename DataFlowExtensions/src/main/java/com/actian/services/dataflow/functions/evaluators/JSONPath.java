/*
   Copyright 2015 Actian Corporation
 
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.actian.services.dataflow.functions.evaluators;

import com.pervasive.datarush.functions.FunctionEvaluator;
import com.pervasive.datarush.tokens.scalar.StringSettable;
import com.pervasive.datarush.tokens.scalar.StringValued;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import net.minidev.json.parser.JSONParser;

import com.jayway.jsonpath.spi.json.JsonProvider;
import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.mapper.JsonSmartMappingProvider;
import com.jayway.jsonpath.spi.mapper.MappingProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;
import com.jayway.jsonpath.Option;

import com.jayway.jsonpath.JsonPathException;

public class JSONPath implements FunctionEvaluator {

    private final StringSettable result;
    private final StringValued expression;
    private final StringValued value;

    public static final Configuration JSON_SMART_CONFIGURATION = Configuration
            .builder()
            .mappingProvider(new JsonSmartMappingProvider())
            .jsonProvider(new JsonSmartJsonProvider(JSONParser.MODE_PERMISSIVE ^ JSONParser.USE_HI_PRECISION_FLOAT))
            .build();

    public JSONPath(StringSettable result, StringValued expression, StringValued value) {
        this.result = result;
        this.expression = expression;
        this.value = value;
    }

    @Override
    public void evaluate() {
        Configuration configuration = JSON_SMART_CONFIGURATION;
        DocumentContext parsedJSON = null;
        Object o = null;

        String output = "";
        try {
            parsedJSON = JsonPath.using(configuration).parse(value.asString());
            o = parsedJSON.read(expression.asString());

            if (o instanceof String) {
                output = o.toString();
            } else if (o instanceof Number) {
                output = o.toString();
            } else if (o instanceof Boolean) {
                output = o.toString();
            } else {
                output = o != null ? configuration.jsonProvider().toJson(o) : "null";
            }
        } catch (Exception ex) {
            output = ex.getMessage();
        }

        result.set(output);
    }
}
