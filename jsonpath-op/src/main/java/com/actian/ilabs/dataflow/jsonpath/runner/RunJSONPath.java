package com.actian.ilabs.dataflow.jsonpath.runner;

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


import static com.pervasive.datarush.io.WriteMode.OVERWRITE;
import static com.pervasive.datarush.types.TokenTypeConstant.*;
import static com.pervasive.datarush.types.TypeUtil.mergeTypes;

import java.sql.*;
// import java.util.*;
import java.util.Collection;
import java.util.List;


import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;

import javax.xml.bind.DatatypeConverter;

import com.pervasive.datarush.encoding.text.DateFormatter;
import com.pervasive.datarush.encoding.text.TimeFormatter;
import com.pervasive.datarush.encoding.text.TimestampFormatter;
import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;
import com.pervasive.datarush.io.WriteMode;
import com.pervasive.datarush.operators.*;
import com.pervasive.datarush.operators.io.textfile.FieldDelimiterSettings;
import com.pervasive.datarush.operators.io.textfile.ReadDelimitedText;
import com.pervasive.datarush.operators.io.textfile.WriteDelimitedText;
import com.pervasive.datarush.ports.physical.*;
import com.pervasive.datarush.ports.record.*;
import com.pervasive.datarush.tokens.TokenUtils;
import com.pervasive.datarush.tokens.scalar.*;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.ScalarTokenType;

import com.pervasive.datarush.types.TokenTypeConstant;
import com.pervasive.datarush.types.TypeUtil;
import org.apache.commons.lang.StringUtils;

public class RunJSONPath extends ExecutableOperator implements RecordPipelineOperator {

	private final RecordPort input = newRecordInput("input");
	private final RecordPort output = newRecordOutput("output");

	private String jsonInputField;
	private String jsonOutputField;
	private String jsonPathExpr;
	
	public RecordPort getInput() {
		return input;
	}
	
	public RecordPort getOutput() {
		return output;
	}

	public String getJsonInputField() { return jsonInputField; }

	public String getJsonOutputField() { return jsonOutputField; }
	
	public String getJsonPathExpr() {
		return jsonPathExpr;
	}

	public void setJsonInputField(String s) { this.jsonInputField = s; }

	public void setJsonOutputField(String s) { this.jsonOutputField = s; }

	public void setJsonPathExpr(String s) {
		this.jsonPathExpr = s;
	}

	public RunJSONPath() {

	}

	@Override
	protected void computeMetadata(StreamingMetadataContext context) {
		//best practice: perform any input validation: should be done first
		// validateInput(context);

		//required: declare our parallelizability.
		//  in this case we use source parallelism as a hint for our parallelism.
		context.parallelize(ParallelismStrategy.NEGOTIATE_BASED_ON_SOURCE);


		//required: declare output type
		//  in this case our output type is the input type plus an additional field
		//  containing the result
		RecordTokenType outputType = mergeTypes(getInput().getType(context), record(STRING(getJsonOutputField())));
		// RecordTokenType outputType = record(STRING("stResult"));
		getOutput().setType(context, outputType);

		//best practice: define output ordering/distribution
		//  in this case we are generating data in a single field so
		//  the ordering is unspecified and the distribution is partial
		RecordMetadata outputMetadata = input.getCombinedMetadata(context);
		output.setOutputDataOrdering(context, DataOrdering.UNSPECIFIED);
	}

	private String formatResult(Object o) {
		String result = null;

		if (o instanceof String) {
			result = "\"" + o + "\"";
		} else if (o instanceof Number) {
			result = o.toString();
		} else if (o instanceof Boolean) {
			result = o.toString();
		} else {
			result = o != null ? Configuration.defaultConfiguration().jsonProvider().toJson(o) : "null";
		}

        return result;
	}

	@Override
	protected void execute(ExecutionContext context) {
		Configuration configuration = Configuration.defaultConfiguration();

		// configuration = configuration.addOptions(Option.ALWAYS_RETURN_LIST);

		RecordInput recordInput = getInput().getInput(context);
		RecordOutput recordOutput = getOutput().getOutput(context);

		StringValued inputField = (StringValued) recordInput.getField(getJsonInputField());
		StringSettable resultField = (StringSettable) recordOutput.getField(getJsonOutputField());

		ScalarValued[] allInputs = recordInput.getFields();
		ScalarSettable[] outputs = TokenUtils.selectFields(recordOutput, recordInput.getType().getNames());

		Object res = null;
		String result = null;

		while(recordInput.stepNext()) {

			try {
				res = JsonPath.using(configuration).parse(inputField.asString()).read(getJsonPathExpr());

				if (res instanceof List) {
					for (Object o: (List) res) {
						//copy original fields as is into the current row buffer
						TokenUtils.transfer(allInputs, outputs);

						resultField.set(formatResult(o));
						recordOutput.push();
					}

				}
				else {
					//copy original fields as is into the current row buffer
					TokenUtils.transfer(allInputs, outputs);

					resultField.set(formatResult(res));
					recordOutput.push();
				}
			} catch (Exception e) {
				// todo write records with parse errors to a reject port
			} finally {
			}

		}

		recordOutput.pushEndOfData();
	}
	
	public static void main(String[] args) {
		LogicalGraph graph = LogicalGraphFactory.newLogicalGraph();
// todo need a better test source
		// Use weather alert data from NOAA as the source
		ReadDelimitedText reader = graph.add(new ReadDelimitedText("http://www.ncdc.noaa.gov/swdiws/csv/warn/id=533623"));
		reader.setHeader(true);
		RunJSONPath runner = graph.add(new RunJSONPath());
		WriteDelimitedText writer = graph.add(new WriteDelimitedText());

		runner.setJsonPathExpr("RECORD(__data, __values, __types) ::= \"<__values><\\n>\"");
		writer.setFieldEndDelimiter("]]");
		writer.setFieldStartDelimiter("[[");
		writer.setFieldDelimiter("|");
		writer.setHeader(false);
		writer.setTarget("stdout:");
		writer.setMode(OVERWRITE);
		graph.connect(reader.getOutput(), runner.getInput());
		graph.connect(runner.getOutput(), writer.getInput());
		graph.compile().run();
	}
}
