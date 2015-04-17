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


import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.GridLayout;

import javax.swing.BorderFactory;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.knime.core.node.InvalidSettingsException;

import com.actian.ilabs.dataflow.jsonpath.runner.RunJSONPath;
import com.pervasive.datarush.knime.coreui.common.CustomDialogComponent;
import com.pervasive.datarush.ports.PortMetadata;

/*package*/ final class JSONPathRunnerNodeDialogPane extends JPanel implements CustomDialogComponent<RunJSONPath> {

	private static final long serialVersionUID = -1744417654171054890L;

	private final JSONPathRunnerNodeSettings settings = new JSONPathRunnerNodeSettings();
    
	private JTextArea jsonPathExpr;
	private JTextField jsonInputField;
	private JTextField jsonOutputField;
    
    @Override
    public JSONPathRunnerNodeSettings getSettings() {
        return settings;
    }
    
    @Override
    public boolean isMetadataRequiredForConfiguration(int portIndex) {
        return true;
    }
    
	@Override
	public Component getComponent() {
		JPanel dialog = new JPanel();
		dialog.setLayout(new BorderLayout());

		JPanel fieldSettings = new JPanel();
		fieldSettings.setLayout(new GridLayout(2,1));

		jsonInputField = createTextField("Input Field Name");
		jsonOutputField = createTextField("Output Field Name");

		fieldSettings.add(jsonInputField);
		fieldSettings.add(jsonOutputField);

		dialog.add(fieldSettings, BorderLayout.NORTH);
		
		jsonPathExpr = createTextArea();
    	JScrollPane scrollPane = new JScrollPane(jsonPathExpr);
    	scrollPane.setBorder(BorderFactory.createTitledBorder("JSONPath Expression"));
    	dialog.add(scrollPane, BorderLayout.CENTER);
    	
    	return dialog;
	}

	@Override
	public void refresh(PortMetadata[] arg0) {
		jsonInputField.setText(settings.jsonInputField.getStringValue());
		jsonOutputField.setText(settings.jsonOutputField.getStringValue());
		jsonPathExpr.setText(settings.jsonPathExpr.getStringValue());
	}

	@Override
	public void validateAndApplySettings() throws InvalidSettingsException {
		settings.jsonInputField.setStringValue(jsonInputField.getText());
		settings.jsonOutputField.setStringValue(jsonOutputField.getText());
		settings.jsonPathExpr.setStringValue(jsonPathExpr.getText());
	}
	
	private JPasswordField createPasswordField(String title) {
		JPasswordField passwordField = new JPasswordField();
		passwordField.setBorder(BorderFactory.createTitledBorder(title));
		return passwordField;
	}
	
    private JTextField createTextField(String title) {
        JTextField textField = new JTextField();
        textField.setBorder(BorderFactory.createTitledBorder(title));
        return textField;
    }
    
	private JTextArea createTextArea() {
		JTextArea textArea = new JTextArea();
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
		return textArea;
	}
}

