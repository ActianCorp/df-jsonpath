package com.actian.ilabs.dataflow.jsonpath.ui.runner;

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

import java.util.Arrays;
import java.util.List;

import com.actian.ilabs.dataflow.jsonpath.runner.RunJSONPath;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.defaultnodesettings.SettingsModel;

import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.ports.PortMetadata;

/*package*/ 
final class RunJSONPathNodeSettings extends AbstractDRSettingsModel<RunJSONPath> {

	public final SettingsModelString jsonInputField = new SettingsModelString("jsonInputField", null);
	public final SettingsModelString jsonOutputField = new SettingsModelString("jsonOutputField", null);
    public final SettingsModelString jsonPathExpr = new SettingsModelString("jsonPathExpr", null);
        
    @Override
    protected List<SettingsModel> getComponentSettings() {
        return Arrays.<SettingsModel>
        asList(jsonInputField, jsonOutputField, jsonPathExpr);
    }

    @Override
    public void configure(PortMetadata[] inputTypes, RunJSONPath operator) throws InvalidSettingsException {
		// todo Input should be a valid field name from the source port schema
		// todo Output should be a valid field name that does not conflict with any of the source fields
    	if (this.jsonInputField.getStringValue() == null || this.jsonInputField.getStringValue().trim().isEmpty()) {
    		throw new InvalidSettingsException("No input field selected!");
    	}
		if (this.jsonOutputField.getStringValue() == null || this.jsonOutputField.getStringValue().trim().isEmpty()) {
			throw new InvalidSettingsException("No output field specified!");
		}
		if (this.jsonPathExpr.getStringValue() == null || this.jsonPathExpr.getStringValue().trim().isEmpty()) {
			throw new InvalidSettingsException("JSONPath expression must not be empty!");
		}

		operator.setJsonInputField(this.jsonInputField.getStringValue());
		operator.setJsonOutputField(this.jsonOutputField.getStringValue());
		operator.setJsonPathExpr(this.jsonPathExpr.getStringValue());
    }
}
