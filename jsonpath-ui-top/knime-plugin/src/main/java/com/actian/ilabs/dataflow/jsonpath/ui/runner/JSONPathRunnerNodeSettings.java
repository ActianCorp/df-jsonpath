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
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;

import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.pervasive.datarush.ports.PortMetadata;

/*package*/ 
final class JSONPathRunnerNodeSettings extends AbstractDRSettingsModel<RunJSONPath> {

	public final SettingsModelStringArray sourceFields = new SettingsModelStringArray("sourceFields", null);
	public final SettingsModelStringArray targetFields = new SettingsModelStringArray("targetFields", null);
	public final SettingsModelStringArray expressions = new SettingsModelStringArray("expressions", null);
	public final SettingsModelStringArray flatmap = new SettingsModelStringArray("flatmap",null);

    @Override
    protected List<SettingsModel> getComponentSettings() {
        return Arrays.<SettingsModel>
        asList(sourceFields, targetFields, flatmap, expressions);
    }

    @Override
    public void configure(PortMetadata[] inputTypes, RunJSONPath operator) throws InvalidSettingsException {
		// todo Input should be a valid field name from the source port schema
		// todo Output should be a valid field name that does not conflict with any of the source fields

		String[] srcfields = sourceFields.getStringArrayValue();
		String[] trgfields = targetFields.getStringArrayValue();
		String[] exprs = expressions.getStringArrayValue();
		String[] flatmapstrs = flatmap.getStringArrayValue();

		if (srcfields == null || trgfields == null || exprs == null) {
			throw new InvalidSettingsException("JSON path expressions are not fully specified.");
		}

		if (srcfields.length != exprs.length || trgfields.length != exprs.length) {
			throw new InvalidSettingsException("JSON path expressions are not fully specified.");
		}

		int srcFieldCount = 0;

		for (int i = 0; i < exprs.length; i++) {
			if (trgfields[i] == null || trgfields[i].length() == 0) {
				throw new InvalidSettingsException("Missing target field name.");
			}
			if (exprs[i] == null || exprs[i].length() == 0) {
				throw new InvalidSettingsException("Missing JSONPath expression.");
			}

			if (srcfields[i] != null && srcfields[i].length() > 0) {
				srcFieldCount++;
			}
		}

		if (srcFieldCount < 1) {
			throw new InvalidSettingsException("Missing source field name.");
		}

		if (srcfields[0] == null || srcfields[0].length() == 0) {
			throw new InvalidSettingsException("Missing source field name for first expression.");
		}

		operator.setSourceFields(this.sourceFields.getStringArrayValue());
		operator.setTargetFields(this.targetFields.getStringArrayValue());
		operator.setExpressions(this.expressions.getStringArrayValue());
		operator.setFlatMap(flatmap.getStringArrayValue());
	}
}
