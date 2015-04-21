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

import com.pervasive.datarush.knime.coreui.common.*;
import org.knime.core.node.InvalidSettingsException;

import com.pervasive.datarush.knime.coreui.common.ColumnMajorTableModel.ColumnModel;
import com.pervasive.datarush.knime.coreui.common.ColumnMajorTableModel.DefaultGenerator;
import com.pervasive.datarush.knime.coreui.common.TableEditorPanel.CopyHandler;
import com.pervasive.datarush.knime.coreui.common.TableEditorPanel.EditHandler;

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
	private static final int FLATTEN_COLUMN = 2;
	private static final int EXPR_COLUMN = 3;

	private final JSONPathRunnerNodeSettings settings = new JSONPathRunnerNodeSettings();

	private RecordTokenType srcType= TokenTypeConstant.RECORD;

	private SourceFieldDomain srcDomain;
	private ColumnModel<String> srcColumn;
	private ColumnModel<String> trgColumn;
	private ColumnModel<Boolean> flattenColumn;
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

		srcDomain= new SourceFieldDomain(TokenTypeConstant.STRING);

		tblModel= new ColumnMajorTableModel();
		srcColumn= tblModel.defineColumn("Source Field", srcDomain);
		trgColumn= tblModel.defineColumn("Output Field", new TextValue());
		flattenColumn = tblModel.defineColumn("Flatten", new BoolValue());
		exprColumn= tblModel.defineColumn("Expression", new TextValue());

		tblExpressionMapping.setModel(tblModel);
		tblExpressionMapping.setRowMembershipHandler(tblModel.createMembershipHandler(new EntryGenerator()));
		tblExpressionMapping.setRowMoveHandler(tblModel.createMoveHandler());
		/*
		 tblExpressionMapping.setRowEditHandler(new EditHandler() {
			@Override
			public void editRow(int row) {
			}
		});
		*/
		tblExpressionMapping.setRowCopyHandler(new CopyHandler() {
			@Override
			public int copyRow(int row) {
				String inField = (String) tblModel.getValueAt(row, SOURCE_COLUMN);
				String outField = (String) tblModel.getValueAt(row, TARGET_COLUMN);
				Boolean flatten = (Boolean) tblModel.getValueAt(row, FLATTEN_COLUMN);
				String expression = (String) tblModel.getValueAt(row, EXPR_COLUMN);
				tblModel.insert(row + 1, new Object[]{inField, outField, flatten, expression});
				return row + 1;
			}
		});
		tblModel.addTableModelListener(this);
	}

	private class EntryGenerator implements DefaultGenerator {
		@Override
		public Object[] getDefaultRow(ColumnMajorTableModel model) {
			return new Object[] { "", "jsonpath" + model.getRowCount(), false, "$..*"};
		}
	}

	private class BoolValue implements ColumnValueDomain<Boolean> {

		@Override
		public Component render(TableCellEditors.TableCellRendererContext context, int row, Object value) {
			// String replacementValue = (String) value;
			// context.getLabel().setText(replacementValue);
			Boolean replacementValue = (Boolean) value;
			context.getCheckBox().setSelected(replacementValue.booleanValue());
			return context.getCheckBox();
		}

		@Override
		public Component getEditorComponent(TableCellEditors.TableCellEditorContext context, int row) {
			// return context.getText();
			return context.getCheckBox();
		}

		@Override
		public void startEditing(TableCellEditors.TableCellEditorContext context, int row, Object value) {
			// context.getText().setText((String) value);
			// context.getText().selectAll();
			Boolean replacementValue = (Boolean) value;
			context.getCheckBox().setSelected(replacementValue.booleanValue());
		}

		@Override
		public Object stopEditing(TableCellEditors.TableCellEditorContext context, int row) {
//			return context.getText().getText();
			Boolean state =  context.getCheckBox().isSelected();
			return state;
		}

	}

	@Override
	public void refresh(PortMetadata[] specs) {
		srcType= ((RecordMetadata) specs[0]).getType();
		srcDomain.setSourceType(srcType);

		srcColumn.setValues(settings.sourceFields.getStringArrayValue());
		trgColumn.setValues(settings.targetFields.getStringArrayValue());
		// flattenColumn.setValues(settings.expload.getBooleanArrayValue());
		exprColumn.setValues(settings.expressions.getStringArrayValue());
	}

	@Override
	public void validateAndApplySettings() throws InvalidSettingsException {
		tblExpressionMapping.finishCurrentEdit();

		//Allow invalid expressions to be saved and perform validation during configure
		settings.expressions.setStringArrayValue(exprColumn.getValues().toArray(new String[0]));

		// Apply
		settings.sourceFields.setStringArrayValue(srcColumn.getValues().toArray(new String[0]));
		settings.targetFields.setStringArrayValue(trgColumn.getValues().toArray(new String[0]));
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
		tblExpressionMapping.setBorder(new TitledBorder(null, "JSON Mappings", TitledBorder.LEADING, TitledBorder.TOP, null, null));

		GroupLayout groupLayout = new GroupLayout(this);
		groupLayout.setHorizontalGroup(
				groupLayout.createParallelGroup(GroupLayout.Alignment.TRAILING)
						.addGroup(groupLayout.createSequentialGroup()
								.addContainerGap()
								.addGroup(groupLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
										.addComponent(tblExpressionMapping, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
								.addContainerGap())
		);
		groupLayout.setVerticalGroup(
				groupLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
						.addGroup(groupLayout.createSequentialGroup()
								.addComponent(tblExpressionMapping, GroupLayout.DEFAULT_SIZE, GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
								.addPreferredGap(LayoutStyle.ComponentPlacement.RELATED)
								.addContainerGap())
		);
		setLayout(groupLayout);
	}

	private TableEditorPanel tblExpressionMapping;

}


