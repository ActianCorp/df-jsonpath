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
import java.util.*;

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
import org.stringtemplate.v4.*;

public class RunJSONPath extends ExecutableOperator implements RecordPipelineOperator {

	private static final Map<ScalarTokenType, String> typeMap;

	static {
		Map<ScalarTokenType, String> map = new HashMap<ScalarTokenType, String>();

		map.put(STRING, "xsd:string");
		map.put(BINARY, "xsd:string");

		map.put(BOOLEAN, "xsd:boolean");

		map.put(NUMERIC, "xsd:decimal");
		map.put(DOUBLE, "xsd:double");
		map.put(FLOAT, "xsd:double");
		map.put(LONG, "xsd:integer");
		map.put(INT, "xsd:integer");
		map.put(TIME, "xsd:time");
		map.put(DATE, "xsd:date");
		map.put(TIMESTAMP, "xsd:dateTime");

		typeMap = map;
	}

	private final RecordPort input = newRecordInput("input");
	private final RecordPort output = newRecordOutput("output");
	
	private String stg;
	
	public RecordPort getInput() {
		return input;
	}
	
	public RecordPort getOutput() {
		return output;
	}
	
	public String getStg() {
		return stg;
	}

	public void setStg(String stg) {
		this.stg = stg;
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
		// RecordTokenType outputType = mergeTypes(getInput().getType(context), record(STRING("stResult")));
		RecordTokenType outputType = record(STRING("stResult"));
		getOutput().setType(context, outputType);

		//best practice: define output ordering/distribution
		//  in this case we are generating data in a single field so
		//  the ordering is unspecified and the distribution is partial
		RecordMetadata outputMetadata = input.getCombinedMetadata(context);
		output.setOutputDataOrdering(context, DataOrdering.UNSPECIFIED);
	}

	private String FormatFieldValue(ScalarInputField field) {
		ScalarTokenType type = field.getType();
		String valueString = "";

		if (type.equals(TokenTypeConstant.BOOLEAN)) {
			BooleanInputField boolField = (BooleanInputField) field;
			valueString = DatatypeConverter.printBoolean(boolField.asBoolean());
		}
		else if (type.equals(TokenTypeConstant.BINARY)) {
			BinaryInputField binField = (BinaryInputField) field;
			valueString = DatatypeConverter.printHexBinary(binField.asBinary());
		}
		else if (type.equals(TokenTypeConstant.CHAR)) {
			CharInputField charField = (CharInputField) field;
			valueString = charField.toString();
		}
		else if (type.equals(TokenTypeConstant.DATE)) {
			DateInputField dateField = (DateInputField) field;
			DateFormatter dateFormatter = new DateFormatter("yyyyMMdd");
			dateFormatter.setSource(dateField);
			valueString = dateFormatter.format();
		}
		else if (type.equals(TokenTypeConstant.DOUBLE)) {
			DoubleInputField doubleField = (DoubleInputField) field;
			valueString = DatatypeConverter.printDouble(doubleField.asDouble());
		}
		else if (type.equals(TokenTypeConstant.FLOAT)) {
			FloatInputField floatField = (FloatInputField) field;
			valueString = DatatypeConverter.printFloat(floatField.asFloat());
		}
		else if (type.equals(TokenTypeConstant.INT)) {
			IntInputField intField = (IntInputField) field;
			valueString = DatatypeConverter.printInt(intField.asInt());
		}
		else if (type.equals(TokenTypeConstant.LONG)) {
			LongInputField longField = (LongInputField) field;
			valueString = DatatypeConverter.printLong(longField.asLong());
		}
		else if (type.equals(TokenTypeConstant.NUMERIC)) {
			NumericInputField numericField = (NumericInputField) field;
			valueString = DatatypeConverter.printDecimal(numericField.asBigDecimal());
		}
		else if (type.equals(TokenTypeConstant.STRING)) {
			StringInputField stringField = (StringInputField) field;
			valueString = DatatypeConverter.printString(stringField.asString());
		}
		else if (type.equals(TokenTypeConstant.TIME)) {
			TimeInputField timeField = (TimeInputField) field;
			TimeFormatter timeFormatter = new TimeFormatter("HHmmss.SSSZ");
			timeFormatter.setSource(timeField);
			valueString = timeFormatter.format();
		}
		else if (type.equals(TokenTypeConstant.TIMESTAMP)) {
			TimestampInputField timestampField = (TimestampInputField) field;
			TimestampFormatter timestampFormatter = new TimestampFormatter("yyyyMMdd'T'HHmmss.SSSZ");
			timestampFormatter.setSource(timestampField);
			valueString = timestampFormatter.format();
		}
		else {
			valueString = "";
		}

		return valueString;
	}

	@Override
	protected void execute(ExecutionContext context) {
		try {
			STGroup group = new STGroupString(getStg());

			RecordInput recordInput = getInput().getInput(context);
			RecordOutput recordOutput = getOutput().getOutput(context);

			//best practice: get handle to output fields
			//  resultField is a handle to the result output field
			//  outputs is an array of all outputs *except* for stResult; corresponds by index to allInputs
			StringSettable resultField = (StringSettable) recordOutput.getField("stResult");
			ScalarSettable[] outputs = TokenUtils.selectFields(recordOutput, getInput().getType(context).getNames());
			ScalarValued[] allInputs = recordInput.getFields();

			ScalarInputField [] fields = recordInput.getFields();

			ArrayList<String> fnameList = new ArrayList<String>(64);

			for (int i = 0; i < fields.length; i++) {
				String fname = fields[i].getName().replaceAll("[^A-Za-z0-9_]","");
				fnameList.add(fname);
			}

			String fnames = StringUtils.join(fnameList, ", ");

			ST headerTemplate = group.getInstanceOf("/HEADER");
			if (headerTemplate != null /* && context.getPartitionInfo().getPartitionID() == 0 */) {
				for (int i = 0; i < fields.length; i++) {
					String typeString = typeMap.containsKey(fields[i].getType()) ? typeMap.get(fields[i].getType()) : "";
					headerTemplate.addAggr("__metadata.{ name, type }", fields[i].getName(), typeString);
				}
			}

			ST footerTemplate = group.getInstanceOf("/FOOTER");
			if (footerTemplate != null) {
				for (int i = 0; i < fields.length; i++) {
					String typeString = typeMap.containsKey(fields[i].getType()) ? typeMap.get(fields[i].getType()) : "";
					footerTemplate.addAggr("__metadata.{ name, type }", fields[i].getName(), typeString);
				}
			}

            long recordCount = 0;

			while(recordInput.stepNext()) {
				ST recordTemplate = group.getInstanceOf("/RECORD");
				if (recordTemplate == null)
					continue;

                recordCount++;

                // Only output the results of the header template if there are records in this partition
                if (headerTemplate != null && recordCount == 1) {
                    String result = headerTemplate.render();
                    resultField.set(result);
                    recordOutput.push();
                }

				ArrayList<String> valueList = new ArrayList<String>(64);
				ArrayList<String> typeList = new ArrayList<String>(64);

				for (int i = 0; i < fields.length; i++) {
					String valueString = FormatFieldValue(fields[i]);
					String typeString = typeMap.containsKey(fields[i].getType()) ? typeMap.get(fields[i].getType()) : "";
					recordTemplate.addAggr("__data.{ name, type, value }", fields[i].getName(), typeString, valueString);
					// recordTemplate.addAggr("__values.{ " + fname + " }", valueString);
					valueList.add(valueString);
					typeList.add(typeString);
					// recordTemplate.addAggr("__types.{ " + fname + " }", typeString);
				}

				recordTemplate.addAggr("__values.{ " + fnames + " }", valueList.toArray());
				recordTemplate.addAggr("__types.{ " + fnames + " }", typeList.toArray());
				String result = recordTemplate.render();
				resultField.set(result);
				recordOutput.push();
			}

			if (footerTemplate != null && recordCount > 0) {
					String result = footerTemplate.render();
					resultField.set(result);
					recordOutput.push();
			}

			//required: signal end-of-data on output
			recordOutput.pushEndOfData();

		} finally {
		}
	}
	
	public static void main(String[] args) {
		LogicalGraph graph = LogicalGraphFactory.newLogicalGraph();

        // Use weather alert data from NOAA as the source
		ReadDelimitedText reader = graph.add(new ReadDelimitedText("http://www.ncdc.noaa.gov/swdiws/csv/warn/id=533623"));
		reader.setHeader(true);
		RunJSONPath runner = graph.add(new RunJSONPath());
		WriteDelimitedText writer = graph.add(new WriteDelimitedText());

        String templateGroup = ""
                + "HEADER(__metadata) ::=<<\n"
                + "<! Generate a comma separated list of input field names !>\n"
                + "<trunc(__metadata):{it|<it.name>,}>\n"
                + "<last(__metadata).name>\n"
                + ">>\n"
                + "\n"
                + "FOOTER(__metadata) ::=<<\n"
                + "<! Generate a line of text as the footer !>\n"
                + "\"This is a footer record\"\n"
                + ">>\n"
                + "\n"
                + "RECORD(__data, __types, __values) ::= <<\n"
                + "\n"
                + "<! Generate a record with the value of input field 'field0' !>\n"
                + "<__values.field0>\n"
                + ">>\n";

		runner.setStg("RECORD(__data, __values, __types) ::= \"<__values><\\n>\"");
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
