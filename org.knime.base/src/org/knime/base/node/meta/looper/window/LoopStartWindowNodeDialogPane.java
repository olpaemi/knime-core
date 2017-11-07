/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 *
 * History
 *   Jun 3, 2010 (wiswedel): created
 */
package org.knime.base.node.meta.looper.window;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ButtonGroup;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;

import org.knime.base.node.meta.looper.window.LoopStartWindowConfiguration.Trigger;
import org.knime.base.node.meta.looper.window.LoopStartWindowConfiguration.WindowDefinition;
import org.knime.base.node.meta.looper.window.LoopStartWindowConfiguration.WindowMode;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.time.duration.DurationValue;
import org.knime.core.data.time.period.PeriodValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 *
 * @author Moritz Heine, KNIME.com, Konstanz, Germany
 */
public class LoopStartWindowNodeDialogPane extends NodeDialogPane {

    private final JRadioButton tumblingWindowRButton;

    private final JRadioButton slidingWindowRButton;

    private final JRadioButton forwardRButton;

    private final JRadioButton centralRButton;

    private final JRadioButton backwardRButton;

    private final JRadioButton eventTrigRButton;

    private final JRadioButton timeTrigRButton;

    private final JSpinner stepSizeSpinner;

    private final JSpinner windowSizeSpinner;

    private final JLabel stepSizeLabel;

    private final JLabel windowSizeLabel;

    private final JTextField timeField;

    private final DialogComponentColumnNameSelection columnSelector;

    /**
     *
     */
    public LoopStartWindowNodeDialogPane() {
        ButtonGroup bg = new ButtonGroup();
        tumblingWindowRButton = new JRadioButton("Tumbling");
        slidingWindowRButton = new JRadioButton("Sliding");

        ActionListener al = new ActionListener() {
            /** {@inheritDoc} */
            @Override
            public void actionPerformed(final ActionEvent e) {
                stepSizeSpinner.setEnabled(slidingWindowRButton.isSelected());
            }
        };

        tumblingWindowRButton.addActionListener(al);
        slidingWindowRButton.addActionListener(al);

        bg.add(tumblingWindowRButton);
        bg.add(slidingWindowRButton);

        bg = new ButtonGroup();

        forwardRButton = new JRadioButton("Forward");
        backwardRButton = new JRadioButton("Backward");
        centralRButton = new JRadioButton("Central");

        bg.add(forwardRButton);
        bg.add(centralRButton);
        bg.add(backwardRButton);

        bg = new ButtonGroup();
        eventTrigRButton = new JRadioButton("Event triggered");
        timeTrigRButton = new JRadioButton("Time triggered");

        bg.add(eventTrigRButton);
        bg.add(timeTrigRButton);

        windowSizeSpinner = new JSpinner(new SpinnerNumberModel(10, 1, Integer.MAX_VALUE, 5));
        stepSizeSpinner = new JSpinner(new SpinnerNumberModel(10, 1, Integer.MAX_VALUE, 10));
        timeField = new JTextField("Time-Duration");

        stepSizeLabel = new JLabel("Step size");
        windowSizeLabel = new JLabel("Window size");

        columnSelector = new DialogComponentColumnNameSelection(createColumnModel(), "time column", 0, false,
            DurationValue.class, PeriodValue.class);

        ActionListener triggerListener = new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                tumblingWindowRButton.setEnabled(eventTrigRButton.isSelected());
                slidingWindowRButton.setEnabled(eventTrigRButton.isSelected());
                timeField.setEnabled(!eventTrigRButton.isSelected());
                columnSelector.getModel().setEnabled(!eventTrigRButton.isSelected());
                windowSizeSpinner.setEnabled(eventTrigRButton.isSelected());
                stepSizeSpinner.setEnabled(eventTrigRButton.isSelected() && slidingWindowRButton.isSelected());
            }
        };

        eventTrigRButton.addActionListener(triggerListener);
        timeTrigRButton.addActionListener(triggerListener);

        slidingWindowRButton.doClick();
        forwardRButton.doClick();
        eventTrigRButton.doClick();

        initLayout();
    }

    /**
     *
     */
    private void initLayout() {
        GridBagLayout gbl = new GridBagLayout();
        JPanel panel = new JPanel(gbl);

        GridBagConstraints constraint = new GridBagConstraints();

        constraint.anchor = GridBagConstraints.LINE_START;
        constraint.ipadx = 2;
        constraint.ipady = 5;
        constraint.insets = new Insets(2, 2, 2, 2);
        constraint.gridx = 1;
        constraint.gridy = 1;

        panel.add(eventTrigRButton, constraint);

        constraint.gridx++;
        panel.add(timeTrigRButton, constraint);

        constraint.gridx = 1;
        constraint.gridy++;
        panel.add(slidingWindowRButton, constraint);

        constraint.gridx++;
        panel.add(tumblingWindowRButton, constraint);

        constraint.gridx = 1;
        constraint.gridy++;
        panel.add(windowSizeLabel, constraint);

        constraint.gridx++;
        panel.add(windowSizeSpinner, constraint);

        constraint.gridx++;
        panel.add(columnSelector.getComponentPanel(), constraint);

        constraint.gridy++;
        panel.add(timeField, constraint);

        constraint.gridx = 1;
        constraint.gridy++;
        panel.add(stepSizeLabel, constraint);

        constraint.gridx++;
        panel.add(stepSizeSpinner, constraint);

        constraint.gridx = 1;
        constraint.gridy++;
        panel.add(forwardRButton, constraint);

        constraint.gridx++;
        panel.add(centralRButton, constraint);

        constraint.gridx++;
        panel.add(backwardRButton, constraint);

        addTab("Configuration", panel);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec[] specs)
        throws NotConfigurableException {
        LoopStartWindowConfiguration config = new LoopStartWindowConfiguration();
        config.loadSettingsInDialog(settings);

        windowSizeSpinner.setValue(config.getWindowSize());
        stepSizeSpinner.setValue(config.getStepSize());

        switch (config.getWindowMode()) {
            case TUMBLING:
                tumblingWindowRButton.doClick();
                break;
            default:
                slidingWindowRButton.doClick();
        }

        switch (config.getWindowDefinition()) {
            case FORWARD:
                forwardRButton.doClick();
                break;
            case BACKWARD:
                backwardRButton.doClick();
                break;
            default:
                centralRButton.doClick();
        }

        switch (config.getTrigger()) {
            case EVENT:
                eventTrigRButton.doClick();
                break;
            default:
                timeTrigRButton.doClick();
        }

        columnSelector.loadSettingsFrom(settings, specs);
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        LoopStartWindowConfiguration config = new LoopStartWindowConfiguration();
        config.setWindowSize((Integer)windowSizeSpinner.getValue());
        config.setStepSize((Integer)stepSizeSpinner.getValue());

        if (tumblingWindowRButton.isSelected()) {
            config.setWindowMode(WindowMode.TUMBLING);
        } else {
            config.setWindowMode(WindowMode.SLIDING);
        }

        if (forwardRButton.isSelected()) {
            config.setWindowDefinition(WindowDefinition.FORWARD);
        } else if (backwardRButton.isSelected()) {
            config.setWindowDefinition(WindowDefinition.BACKWARD);
        } else {
            config.setWindowDefinition(WindowDefinition.CENTRAL);
        }

        if (eventTrigRButton.isSelected()) {
            config.setTrigger(Trigger.EVENT);
        } else {
            config.setTrigger(Trigger.TIME);
        }

        config.saveSettingsTo(settings);
        columnSelector.saveSettingsTo(settings);
    }

    /**
     * @return settings model for column selection
     */
    static final SettingsModelString createColumnModel() {
        return new SettingsModelString("selectedTimeColumn", null);
    }

}
