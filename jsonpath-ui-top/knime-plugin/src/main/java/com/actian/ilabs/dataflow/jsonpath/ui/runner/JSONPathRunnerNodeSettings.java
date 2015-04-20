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
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;

import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.ports.PortMetadata;

/*package*/ 
final class JSONPathRunnerNodeSettings extends AbstractDRSettingsModel<RunJSONPath> {

	public final SettingsModelStringArray sourceFields = new SettingsModelStringArray("sourceFields", null);
	public final SettingsModelStringArray targetFields = new SettingsModelStringArray("targetFields", null);
	public final SettingsModelStringArray expressions = new SettingsModelStringArray("expressions", null);
	public final SettingsModelBoolean dropUnderived = new SettingsModelBoolean("dropUnderived",false);

    @Override
    protected List<SettingsModel> getComponentSettings() {
        return Arrays.<SettingsModel>
        asList(sourceFields, targetFields, expressions, dropUnderived);
    }

    @Override
    public void configure(PortMetadata[] inputTypes, RunJSONPath operator) throws InvalidSettingsException {
		// todo Input should be a valid field name from the source port schema
		// todo Output should be a valid field name that does not conflict with any of the source fields

		String[] srcfields = sourceFields.getStringArrayValue();
		String[] trgfields = targetFields.getStringArrayValue();
		String[] exprs = expressions.getStringArrayValue();

		if (srcfields == null || trgfields == null || exprs == null) {
			throw new InvalidSettingsException("JSON path expressions not fully specified.");
		}

		if (srcfields.length != exprs.length || trgfields.length != exprs.length) {
			throw new InvalidSettingsException("JSON path expressions not fully specified.");
		}

		//operator.setJsonInputField(this.jsonInputField.getStringValue());
		//operator.setJsonOutputField(this.jsonOutputField.getStringValue());
		//operator.setJsonPathExpr(this.jsonPathExpr.getStringValue());
		// operator.setDropUnderivedFields(dropUnderived.getBooleanValue());
	}
}