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

import com.pervasive.datarush.knime.core.framework.AbstractDRSettingsModel;
import com.actian.ilabs.dataflow.jsonpath.runner.RunJSONPath;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import java.util.List;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import java.util.Arrays;
import com.pervasive.datarush.ports.PortMetadata;
import org.knime.core.node.InvalidSettingsException;

/*package*/ 
final class RunJSONPathNodeSettings extends AbstractDRSettingsModel<RunJSONPath> {

    public final SettingsModelString stg = new SettingsModelString("stg", null);
        
    @Override
    protected List<SettingsModel> getComponentSettings() {
        return Arrays.<SettingsModel>
        asList(stg);
    }

    @Override
    public void configure(PortMetadata[] inputTypes, RunJSONPath operator) throws InvalidSettingsException {
    	if (this.stg.getStringValue() == null || this.stg.getStringValue().trim().isEmpty()) {
    		throw new InvalidSettingsException("String template definition must not be empty!");
    	}
    	
        operator.setStg(this.stg.getStringValue());
    }
}
