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


import java.awt.Component;
import java.awt.Dimension;
import java.io.StringReader;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;

import org.knime.core.node.InvalidSettingsException;

import com.pervasive.datarush.knime.coreui.common.ColumnMajorTableModel;
import com.pervasive.datarush.knime.coreui.common.ColumnMajorTableModel.ColumnModel;
import com.pervasive.datarush.knime.coreui.common.ColumnMajorTableModel.DefaultGenerator;
import com.pervasive.datarush.knime.coreui.common.ColumnMajorTableModel.EditCondition;
import com.pervasive.datarush.knime.coreui.common.ColumnValueDomain;
import com.pervasive.datarush.knime.coreui.common.CustomDialogComponent;
import com.pervasive.datarush.knime.coreui.common.TableEditorPanel;
import com.pervasive.datarush.knime.coreui.common.TableEditorPanel.CopyHandler;
import com.pervasive.datarush.knime.coreui.common.TableEditorPanel.EditHandler;
import com.pervasive.datarush.knime.coreui.common.TextValue;

import com.pervasive.datarush.ports.PortMetadata;
import com.pervasive.datarush.ports.record.RecordMetadata;
import com.pervasive.datarush.types.Field;
import com.pervasive.datarush.types.RecordTokenType;
import com.pervasive.datarush.types.RecordTokenTypeBuilder;
import com.pervasive.datarush.types.ScalarTokenType;
import com.pervasive.datarush.types.TokenTypeConstant;

import com.actian.ilabs.dataflow.jsonpath.runner.RunJSONPath;


/*package*/ final class JSONPathRunnerNodeDialogPane extends JPanel implements CustomDialogComponent<RunJSONPath>, TableModelListener {

	private static final long serialVersionUID = -1744417654171054890L;

	private static final int SOURCE_COLUMN = 0;
	private static final int TARGET_COLUMN = 1;
	private static final int EXPR_COLUMN = 2;

	private final JSONPathRunnerNodeSettings settings = new JSONPathRunnerNodeSettings();
    
	private ColumnModel<String> srcColumn;
	private ColumnModel<String> trgColumn;
	private ColumnModel<String> exprColumn;
	private ColumnMajorTableModel tblModel;

    @Override
    public JSONPathRunnerNodeSettings getSettings() {
        return settings;
    }
    
    @Override
    public boolean isMetadataRequiredForConfiguration(int portIndex) {
        return true;
    }

	public JSONPathRunnerNodeDialogPane() {
		initComponents();

		tblModel= new ColumnMajorTableModel();
		srcColumn= tblModel.defineColumn("Source Field", new TextValue());
		trgColumn= tblModel.defineColumn("Output Field", new TextValue());
		exprColumn= tblModel.defineColumn("Expression", new TextValue());

		tblExpressionMapping.setModel(tblModel);
		tblExpressionMapping.setRowMembershipHandler(tblModel.createMembershipHandler(new EntryGenerator()));
		tblExpressionMapping.setRowMoveHandler(tblModel.createMoveHandler());
		tblExpressionMapping.setRowEditHandler(new EditHandler() {
			@Override
			public void editRow(int row) {
			}
		});
		tblExpressionMapping.setRowCopyHandler(new CopyHandler() {
			@Override
			public int copyRow(int row) {
				String inField = (String) tblModel.getValueAt(row, SOURCE_COLUMN);
				String outField = (String) tblModel.getValueAt(row, TARGET_COLUMN);
				String expression = (String) tblModel.getValueAt(row, EXPR_COLUMN);
				tblModel.insert(row + 1, new Object[]{inField, outField, expression, ""});
				return row + 1;
			}
		});
		tblModel.addTableModelListener(this);
	}

	private class EntryGenerator implements DefaultGenerator {
		@Override
		public Object[] getDefaultRow(ColumnMajorTableModel model) {
			return new Object[] { "field" + model.getRowCount(), "0", "" };
		}
	}

	@Override
	public void refresh(PortMetadata[] specs) {
		srcColumn.setValues(settings.sourceFields.getStringArrayValue());
		trgColumn.setValues(settings.targetFields.getStringArrayValue());
		exprColumn.setValues(settings.expressions.getStringArrayValue());
		chckbxDropUnderivedFields.setSelected(settings.dropUnderived.getBooleanValue());
	}

	@Override
	public void validateAndApplySettings() throws InvalidSettingsException {
		tblExpressionMapping.finishCurrentEdit();

		//Allow invalid expressions to be saved and perform validation during configure
		settings.expressions.setStringArrayValue(exprColumn.getValues().toArray(new String[0]));

		// Apply
		settings.sourceFields.setStringArrayValue(srcColumn.getValues().toArray(new String[0]));
		settings.targetFields.setStringArrayValue(trgColumn.getValues().toArray(new String[0]));
		settings.dropUnderived.setBooleanValue(chckbxDropUnderivedFields.isSelected());
	}
	@Override
	public Component getComponent() {
		return this;
	}

	@Override
	public void tableChanged(TableModelEvent evt) {

		// Start with first row changed
		int start = evt.getFirstRow();
		// Always end with the last row in the table, in case any subsequent expressions use results from the changed expressions.
		int end = tblModel.getRowCount();

		if (evt.getType() == TableModelEvent.INSERT) {
			tblExpressionMapping.requestTableFocus();
			tblExpressionMapping.setSelectedRow(start);
			tblExpressionMapping.setSelectedColumn(0);
		}

		// Check for errors
		for (int i = start; i < end; i++) {
			String source = (String) tblModel.getValueAt(i, SOURCE_COLUMN);
			String target = (String) tblModel.getValueAt(i, TARGET_COLUMN);
			String expression = (String) tblModel.getValueAt(i, EXPR_COLUMN);
			StringBuilder errors = new StringBuilder();

			if (source == null || source.length() == 0) {
				errors.append("<p>Source field name cannot be blank.</p>");
			}

			if (target == null || target.length() == 0) {
				errors.append("<p>Target field name cannot be blank.</p>");
			}

			if (expression == null || expression.length() == 0) {
				errors.append("<p>JSON path expression cannot be blank.</p>");
			}
		}
	}

	private void initComponents() {
		tblExpressionMapping = new TableEditorPanel();
		tblExpressionMapping.setBorder(new TitledBorder(null, "Derived Outputs", TitledBorder.LEADING, TitledBorder.TOP, null, null));

		chckbxDropUnderivedFields = new JCheckBox("Drop Underived Fields");

		GroupLayout groupLayout = new GroupLayout(this);
		groupLayout.setHorizontalGroup(
				groupLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
						.addGroup(groupLayout.createSequentialGroup()
								.addContainerGap()
								.addGroup(groupLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
										.addComponent(tblExpressionMapping, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
										.addComponent(chckbxDropUnderivedFields))
								.addContainerGap())
		);
		groupLayout.setVerticalGroup(
				groupLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
						.addGroup(groupLayout.createSequentialGroup()
								.addComponent(tblExpressionMapping, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
								.addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
								.addComponent(chckbxDropUnderivedFields)
								.addContainerGap())
		);
		setLayout(groupLayout);
	}

	private TableEditorPanel tblExpressionMapping;
	private JCheckBox chckbxDropUnderivedFields;

}


