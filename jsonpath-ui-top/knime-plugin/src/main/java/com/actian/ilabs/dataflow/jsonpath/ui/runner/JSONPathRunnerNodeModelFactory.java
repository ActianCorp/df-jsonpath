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

import com.actian.ilabs.dataflow.jsonpath.runner.RunJSONPath;
import com.pervasive.datarush.knime.core.framework.AbstractDRNodeFactory;
import com.pervasive.datarush.knime.core.framework.DRNodeModel;
import com.pervasive.datarush.knime.coreui.common.CustomDRNodeDialogPane;

public final class JSONPathRunnerNodeModelFactory extends AbstractDRNodeFactory<RunJSONPath> {

    @Override
    protected CustomDRNodeDialogPane<RunJSONPath> createNodeDialogPane() {
    	CustomDRNodeDialogPane<RunJSONPath> dialog = new CustomDRNodeDialogPane<RunJSONPath>(new RunJSONPath(), new JSONPathRunnerNodeDialogPane());
    	dialog.setDefaultTabTitle("Properties");
        return dialog;
    }

    @Override
    public DRNodeModel<RunJSONPath> createDRNodeModel() {
        return new DRNodeModel<RunJSONPath>( new RunJSONPath(), new JSONPathRunnerNodeSettings());
    }

    @Override
    protected boolean hasDialog() {
        return true;
    }
}
