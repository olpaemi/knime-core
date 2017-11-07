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

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.JTextField;
import javax.swing.SpinnerNumberModel;

import org.knime.base.node.meta.looper.window.LoopStartWindowConfiguration.Trigger;
import org.knime.base.node.meta.looper.window.LoopStartWindowConfiguration.WindowDefinition;
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

//    private final JRadioButton tumblingWindowRButton;
//
//    private final JRadioButton slidingWindowRButton;

    private final JRadioButton forwardRButton;

    private final JRadioButton centralRButton;

    private final JRadioButton backwardRButton;

    private final JRadioButton eventTrigRButton;

    private final JRadioButton timeTrigRButton;

    private final JSpinner stepSizeSpinner;

    private final JSpinner windowSizeSpinner;

    private final JLabel stepSizeLabel;

    private final JLabel windowSizeLabel;

    private final JLabel windowTimeLabel;

    private final JLabel startTimeLabel;

    private final JTextField timeWindow;

    private final JTextField startTime;

    private final DialogComponentColumnNameSelection columnSelector;


    /**
     *
     */
    public LoopStartWindowNodeDialogPane() {
        ButtonGroup bg = new ButtonGroup();

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

        timeWindow = new JTextField("Time-Duration");
        startTime = new JTextField("start");

        stepSizeLabel = new JLabel("Step size");
        windowSizeLabel = new JLabel("Window size");

        windowTimeLabel = new JLabel("Window time");
        startTimeLabel = new JLabel("Starting interval");

        columnSelector = new DialogComponentColumnNameSelection(createColumnModel(), "time column", 0, false,
            DurationValue.class, PeriodValue.class);

        ActionListener triggerListener = new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                /* Time triggered */
                timeWindow.setEnabled(!eventTrigRButton.isSelected());
                columnSelector.getModel().setEnabled(!eventTrigRButton.isSelected());
                startTime.setEnabled(!eventTrigRButton.isSelected());

                /* Event triggered */
                windowSizeSpinner.setEnabled(eventTrigRButton.isSelected());
                stepSizeSpinner.setEnabled(eventTrigRButton.isSelected());
                forwardRButton.setEnabled(eventTrigRButton.isSelected());
                backwardRButton.setEnabled(eventTrigRButton.isSelected());
                centralRButton.setEnabled(eventTrigRButton.isSelected());
            }
        };

        eventTrigRButton.addActionListener(triggerListener);
        timeTrigRButton.addActionListener(triggerListener);

//        slidingWindowRButton.doClick();
        forwardRButton.doClick();
        eventTrigRButton.doClick();

        initLayout();
    }

    /**
     *
     */
    private void initLayout() {
        JPanel panel = new JPanel(new GridBagLayout());

        GridBagConstraints constraint = new GridBagConstraints();

        constraint.anchor = GridBagConstraints.LINE_START;
        constraint.gridx = 1;
        constraint.gridy = 1;

        panel.add(eventTrigRButton, constraint);

        constraint.gridx++;
        panel.add(timeTrigRButton, constraint);

        /* Event sub-panel */
        JPanel eventPanel = new JPanel(new GridBagLayout());
        GridBagConstraints subConstraint = new GridBagConstraints();

        subConstraint.ipadx = 2;
        subConstraint.ipady = 5;
        subConstraint.insets = new Insets(2, 2, 2, 2);
        subConstraint.gridx = 1;
        subConstraint.gridy = 1;

        eventPanel.add(windowSizeLabel, subConstraint);

        subConstraint.gridx++;
        eventPanel.add(windowSizeSpinner, subConstraint);

        subConstraint.gridx--;
        subConstraint.gridy++;
        eventPanel.add(stepSizeLabel, subConstraint);

        subConstraint.gridx++;
        eventPanel.add(stepSizeSpinner, subConstraint);

        subConstraint.gridx--;
        subConstraint.gridy++;
        eventPanel.add(forwardRButton, subConstraint);

        subConstraint.gridx++;
        eventPanel.add(centralRButton, subConstraint);

        subConstraint.gridx++;
        eventPanel.add(backwardRButton, subConstraint);

        eventPanel.setBorder(BorderFactory.createTitledBorder("Event Triggered"));

        constraint.gridx--;
        constraint.gridy++;
        panel.add(eventPanel, constraint);

        /* Time sub-panel */
        JPanel timePanel = new JPanel(new GridBagLayout());

        subConstraint.gridx = 1;
        subConstraint.gridy = 1;

        timePanel.add(columnSelector.getComponentPanel(), subConstraint);

        subConstraint.gridy++;
        timePanel.add(windowTimeLabel, subConstraint);

        subConstraint.gridx++;
        timePanel.add(timeWindow,subConstraint);

        subConstraint.gridx--;
        subConstraint.gridy++;
        timePanel.add(startTimeLabel, subConstraint);

        subConstraint.gridx++;
        timePanel.add(startTime, subConstraint);

        timePanel.setBorder(BorderFactory.createTitledBorder("Time triggered"));

        constraint.gridy++;
        panel.add(timePanel, constraint);

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

//        switch (config.getWindowMode()) {
//            case TUMBLING:
//                tumblingWindowRButton.doClick();
//                break;
//            default:
//                slidingWindowRButton.doClick();
//        }

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
