package com.actian.services.dataflow.operators;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;


import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

import com.jayway.jsonpath.Option;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonMethod;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;

import com.pervasive.datarush.annotations.PortDescription;
import com.pervasive.datarush.annotations.PropertyDescription;

import net.minidev.json.parser.JSONParser;

import com.jayway.jsonpath.spi.json.GsonJsonProvider;
import com.jayway.jsonpath.spi.json.JsonSmartJsonProvider;
import com.jayway.jsonpath.spi.mapper.JsonSmartMappingProvider;
import com.jayway.jsonpath.spi.mapper.GsonMappingProvider;

import com.pervasive.datarush.graphs.LogicalGraph;
import com.pervasive.datarush.graphs.LogicalGraphFactory;

import com.pervasive.datarush.operators.*;
import com.pervasive.datarush.operators.io.textfile.ReadDelimitedText;
import com.pervasive.datarush.operators.io.textfile.WriteDelimitedText;
import com.pervasive.datarush.ports.physical.*;
import com.pervasive.datarush.ports.record.*;
import com.pervasive.datarush.tokens.TokenUtils;
import com.pervasive.datarush.tokens.scalar.*;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.RecordTokenTypeBuilder;
import org.apache.commons.lang.BooleanUtils;

@JsonSerialize(include=Inclusion.NON_DEFAULT)
public class RunJSONPath extends ExecutableOperator implements RecordPipelineOperator {

	private final RecordPort input = newRecordInput("input");
	private final RecordPort output = newRecordOutput("output");
	private final RecordPort reject = newRecordOutput("reject");

	private String[] expressions = null;
	private String[] sourceFields = null;
	private String[] targetFields = null;
	private String[] flatmapStrings = null;
	private boolean excludeSourceFields = false;
	private boolean nullMissingLeaf = false;

	private Boolean[] flatmap;

	@PortDescription("Source records")
	public RecordPort getInput() {
		return input;
	}

	@PortDescription("Output records")
	public RecordPort getOutput() {
		return output;
	}

	@PortDescription("Rejected records")
	public RecordPort getReject() {
		return reject;
	}

	@PropertyDescription("JSONPath expression list")
	public String[] getExpressions() {
		return expressions;
	}
	public void setExpressions(String[] s) {
		this.expressions = s;
	}

	@PropertyDescription("Flat Map indicator list")
	public String[] getFlatMap() {
		return flatmapStrings;
	}

	public void setFlatMap(String[] s) {
		this.flatmapStrings = s;
		flatmap = StringArray2BooleanArray(s);
	}

	@PropertyDescription("JSON source field list")
	public String[] getSourceFields() {
		return sourceFields;
	}

	public void setSourceFields(String[] s) {
		this.sourceFields = s;
	}

	@PropertyDescription("JSONPath result field list")
	public String[] getTargetFields() {
		return targetFields;
	}

	public void setTargetFields(String[] s) {
		this.targetFields = s;
	}

	@PropertyDescription("Return null for missing leaf nodes")
	public boolean getNullMissingLeaf() { return this.nullMissingLeaf; }
	public void setNullMissingLeaf(boolean b) { this.nullMissingLeaf = b; }

	@PropertyDescription("Exclude JSON source fields from output")
	public boolean getExcludeSourceFields() { return this.excludeSourceFields; }
	public void setExcludeSourceFields(boolean b) {this.excludeSourceFields = b; }

	public RunJSONPath() {
        this.nullMissingLeaf = false;
        this.excludeSourceFields = false;
	}

	@Override
	protected void computeMetadata(StreamingMetadataContext context) {
		//best practice: perform any input validation: should be done first
		// validateInput(context);

		//required: declare our parallelizability.
		//  in this case we use source parallelism as a hint for our parallelism.
		context.parallelize(ParallelismStrategy.NEGOTIATE_BASED_ON_SOURCE);


		// Convert the list of output field names to a schema
		RecordTokenTypeBuilder typeBuilder = new RecordTokenTypeBuilder();
		if (targetFields != null) {
			for (String t : targetFields) {
				typeBuilder.addField(field(STRING, t));
			}
		}

		//required: declare output type
		//  in this case our output type is the input type plus an additional field
		//  containing the result
        if (excludeSourceFields) {
            getOutput().setType(context, typeBuilder.toType());
        } else {
            RecordTokenType outputType = mergeTypes(getInput().getType(context), typeBuilder.toType());
            getOutput().setType(context, outputType);
        }
		RecordTokenType rejectType = mergeTypes(getInput().getType(context), record(STRING("jsonPathErrorText")));
		getReject().setType(context, rejectType);

		//best practice: define output ordering/distribution
		//  in this case we are generating data in a single field so
		//  the ordering is unspecified and the distribution is partial
		RecordMetadata outputMetadata = input.getCombinedMetadata(context);
		output.setOutputDataOrdering(context, DataOrdering.UNSPECIFIED);
		reject.setOutputDataOrdering(context, DataOrdering.UNSPECIFIED);
	}

	private String formatResult(Configuration configuration, Object o) {
		String result = null;

		if (o instanceof String) {
			result = o.toString();
		} else if (o instanceof Number) {
			result = o.toString();
		} else if (o instanceof Boolean) {
			result = o.toString();
		} else {
			result = o != null ? configuration.jsonProvider().toJson(o) : "null";
		}

        return result;
	}

	// Check the operator configuration
	private boolean checkConfig() {
		// Make sure all of the mapping arrays exist
		if (sourceFields == null || targetFields == null || expressions == null) {
			return false;
		}

		// Make sure the mapping array lengths are consistent
		if (targetFields.length != expressions.length) {
			return false;
		}

		int srcFieldCount = 0;

		// Make sure all of the mappings have a target field name and JSONPath expression
		for (int i = 0; i < expressions.length; i++) {
			if (targetFields[i] == null || targetFields[i].length() == 0) {
				return false;
			}
			if (expressions[i] == null || expressions[i].length() == 0) {
				return false;
			}

			// Count the source fields
			if (i < sourceFields.length && sourceFields[i] != null && sourceFields[i].length() > 0) {
				srcFieldCount++;
			}
		}

		// Make sure at least one source field is specified for the mappings
		if (srcFieldCount < 1) {
			return false;
		}

		// Make sure the source field for the first mapping is specified
		if (sourceFields[0] == null || sourceFields[0].length() == 0) {
			return false;
		}

        return true;
	}

	@Override
	protected void execute(ExecutionContext context) {

		Configuration configuration = Configuration
			.builder()
			.mappingProvider(new JsonSmartMappingProvider())
			.jsonProvider(new JsonSmartJsonProvider(JSONParser.MODE_PERMISSIVE ^ JSONParser.USE_HI_PRECISION_FLOAT))
			.build();


		if(nullMissingLeaf){
			configuration = configuration.addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL);
		}


		RecordInput recordInput = getInput().getInput(context);
		RecordOutput recordOutput = getOutput().getOutput(context);
		RecordOutput recordReject = getReject().getOutput(context);

		ScalarValued[] allInputs = recordInput.getFields();
		ScalarSettable[] outputs = TokenUtils.selectFields(recordOutput, recordInput.getType().getNames());
		ScalarSettable[] rejects = TokenUtils.selectFields(recordReject, recordInput.getType().getNames());
		StringSettable jsonPathErrorText = (StringSettable) recordReject.getField(recordReject.size() - 1);

		// Quit early if the operator configuration isn't valid
		if (checkConfig() == false) {
			recordOutput.pushEndOfData();
			recordReject.pushEndOfData();
			return;
		}

		int flatcnt = 0;

        int resultOffset = 0;

        if (!excludeSourceFields)
            resultOffset = allInputs.length;

		// Count the number of output fields being flat mapped
		for (Boolean f : flatmap) {
			if (f)
				flatcnt++;
		}

		while (recordInput.stepNext()) {
			List<Object> results = new ArrayList<Object>();

			int largestListSize = 0;
			boolean rejected = false;

			DocumentContext parsedJSON = null;
			StringValued inputField = null;

			// Evaluate each of the JSONPath expressions
			for (int i = 0; i < targetFields.length; i++) {


				try {

					// Use the previously parsed object if a new is not specified.
					if (i < sourceFields.length && sourceFields[i] != null && sourceFields[i].length() > 0) {

						// Only parse the source if it is different from the previously parsed JSON object.
						if (inputField != recordInput.getField(sourceFields[i])) {
							inputField = (StringValued) recordInput.getField(sourceFields[i]);
							parsedJSON = JsonPath.using(configuration).parse(inputField.asString());
						}
					}
				}
				catch (Exception e)
				{
					// Copy the original input record fields to the corresponding reject record fields
					TokenUtils.transfer(allInputs, rejects);
					jsonPathErrorText.set(e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()));
					recordReject.push();

					rejected = true;
					break;
				}

				String jsonPathExpr = expressions[i];

				Object res = null;

				try {
					res = parsedJSON.read(jsonPathExpr);
				}
				catch (Exception e)
				{
					// Copy the original input record fields to the corresponding reject record fields
					TokenUtils.transfer(allInputs, rejects);
					jsonPathErrorText.set(e.getMessage() + "\n" + Arrays.toString(e.getStackTrace()));
					recordReject.push();

					rejected = true;
					break;
				}
				finally {

					results.add(res);

					if (res != null && res instanceof List) {
						List list = (List) res;
						if (list.size() > largestListSize) {
							largestListSize = list.size();
						}
					}
				}
			}

			// Continue with the next record if we rejected the current one
			if (rejected)
				continue;

			if (flatcnt == 0) {
				// No flattening to do.

				// Copy the original input record fields to the corresponding output record fields
				if (!excludeSourceFields) {
                    TokenUtils.transfer(allInputs, outputs);
                }

				for (int i = 0; i < targetFields.length; i++){

					// The output record was generated by merging the input record with a list of new fields
					// and new fields might have slightly different names if there were any name conflicts.
					// We need to compute the offset of the current output field rather than look it up by name.
					StringSettable resultField = (StringSettable) recordOutput.getField(i + resultOffset);

					resultField.set(formatResult(configuration, results.get(i)));
				}
				recordOutput.push();
			}
			else {
				// Generate one output row for each element of the largest result list
				for (int f = 0; f < largestListSize; f++) {

					// Copy the original input record fields to the corresponding output record fields
					if (!excludeSourceFields) {
                        TokenUtils.transfer(allInputs, outputs);
                    }

					for (int i = 0; i < targetFields.length; i++){

						// The output record was generated by merging the input record with a list of new fields
						// and new fields might have slightly different names if there were any name conflicts.
						// We need to compute the offset of the current output field rather than look it up by name.
						StringSettable resultField = (StringSettable) recordOutput.getField(i + resultOffset);

						Object o = results.get(i);

						// See if we are flat mapping this result
						if (flatmap[i] && o instanceof List) {
							List l = (List) o;
							if (f < l.size()) {
								resultField.set(formatResult(configuration, l.get(f)));
							}
							else {
								resultField.set((String) null);
							}
						} else {
							resultField.set(formatResult(configuration, results.get(i)));
						}

					}
					recordOutput.push();
				}
			}
		}

		recordOutput.pushEndOfData();
		recordReject.pushEndOfData();
	}

	public static Boolean[] StringArray2BooleanArray(String[] strings) {
		List<Boolean> booleans = new ArrayList<Boolean>();

		if (strings != null) {
			for (String s : strings) {
				booleans.add(BooleanUtils.toBoolean(s));
			}
		}

		Boolean[] result = new Boolean[booleans.size()];

		return booleans.toArray(result);
	}

	public static String[] BooleanArray2StringArray(Boolean[] booleans) {
		List<String> strings = new ArrayList<String>();

		if (booleans != null) {
			for (Boolean b : booleans) {
				strings.add(b ? "true" : "false");
			}
		}

		String[] result = new String[strings.size()];

		return strings.toArray(result);
	}



	public static void main(String[] args) {
		LogicalGraph graph = LogicalGraphFactory.newLogicalGraph();
		ReadDelimitedText reader = graph.add(new ReadDelimitedText("https://raw.githubusercontent.com/ActianCorp/df-jsonpath/master/examples/KNIME/twitterdemo.txt"));
		reader.setHeader(false);
		reader.setFieldDelimiter("\uffff");
		reader.setRecordSeparator("\r\n");
		reader.setFieldSeparator("\u0000");

		String[] sflds = {"field0"};
		String[] tflds = {"id", "hashtags"};
		String[] expr = { "$.id", "$.entities.hashtags..text"};
		String[] flatmap = { "false", "true"};
		RunJSONPath runner = graph.add(new RunJSONPath());
		runner.setExpressions(expr);
		runner.setFlatMap(flatmap);
		runner.setSourceFields(sflds);
		runner.setTargetFields(tflds);
		WriteDelimitedText writer = graph.add(new WriteDelimitedText());
		writer.setFieldEndDelimiter("]]");
		writer.setFieldStartDelimiter("[[");
		writer.setFieldDelimiter(",");
		writer.setHeader(false);
		writer.setTarget("stdout:");
		writer.setMode(OVERWRITE);
		graph.connect(reader.getOutput(), runner.getInput());
		graph.connect(runner.getOutput(), writer.getInput());
		graph.compile().run();
	}
}
