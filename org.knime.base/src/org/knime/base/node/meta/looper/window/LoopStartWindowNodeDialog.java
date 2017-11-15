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

import java.awt.Component;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.time.Duration;
import java.time.format.DateTimeParseException;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JCheckBox;
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
import org.knime.core.data.time.localdate.LocalDateValue;
import org.knime.core.data.time.localdatetime.LocalDateTimeValue;
import org.knime.core.data.time.localtime.LocalTimeValue;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.time.util.DurationPeriodFormatUtils;

/**
 * Dialog pane for the Window Loop Start node.
 *
 * @author Moritz Heine, KNIME GmbH, Konstanz, Germany
 */
final class LoopStartWindowNodeDialog extends NodeDialogPane {

    private final JRadioButton m_forwardRButton;

    private final JRadioButton m_centralRButton;

    private final JRadioButton m_backwardRButton;

    private final JRadioButton m_eventTrigRButton;

    private final JRadioButton m_timeTrigRButton;

    private final JSpinner m_stepSizeSpinner;

    private final JSpinner m_windowSizeSpinner;

    private final JLabel m_stepSizeLabel;

    private final JLabel m_windowSizeLabel;

    private final JLabel m_windowTimeLabel;

    private final JLabel m_startTimeLabel;

    private final JTextField m_timeWindow;

    private final JTextField m_startTime;

    private final JCheckBox m_limitWindowCheckBox;

    private final DialogComponentColumnNameSelection m_columnSelector;

    /**
     *
     */
    public LoopStartWindowNodeDialog() {
        ButtonGroup bg = new ButtonGroup();

        m_forwardRButton = new JRadioButton("Forward");
        m_backwardRButton = new JRadioButton("Backward");
        m_centralRButton = new JRadioButton("Central");

        bg.add(m_forwardRButton);
        bg.add(m_centralRButton);
        bg.add(m_backwardRButton);

        bg = new ButtonGroup();
        m_eventTrigRButton = new JRadioButton("Event triggered");
        m_timeTrigRButton = new JRadioButton("Time triggered");

        bg.add(m_eventTrigRButton);
        bg.add(m_timeTrigRButton);

        m_windowSizeSpinner = new JSpinner(new SpinnerNumberModel(10, 1, Integer.MAX_VALUE, 5));
        m_stepSizeSpinner = new JSpinner(new SpinnerNumberModel(10, 1, Integer.MAX_VALUE, 10));

        m_timeWindow = new JTextField();
        m_startTime = new JTextField();

        m_stepSizeLabel = new JLabel("Step size");
        m_windowSizeLabel = new JLabel("Window size");

        m_windowTimeLabel = new JLabel("Window time");
        m_startTimeLabel = new JLabel("Starting interval");

        m_limitWindowCheckBox = new JCheckBox("Limit window to table");
        m_limitWindowCheckBox.setSelected(false);

        m_columnSelector =
            new DialogComponentColumnNameSelection(createColumnModel(), "time column", 0, false, DurationValue.class,
                LocalTimeValue.class, LocalDateTimeValue.class, LocalDateValue.class, ZonedDateTimeValue.class);

        ActionListener triggerListener = new ActionListener() {

            @Override
            public void actionPerformed(final ActionEvent e) {
                /* Time triggered */
                m_timeWindow.setEnabled(!m_eventTrigRButton.isSelected());
                m_columnSelector.getModel().setEnabled(!m_eventTrigRButton.isSelected());
                m_startTime.setEnabled(!m_eventTrigRButton.isSelected());

                /* Event triggered */
                m_windowSizeSpinner.setEnabled(m_eventTrigRButton.isSelected());
                m_stepSizeSpinner.setEnabled(m_eventTrigRButton.isSelected());
                m_forwardRButton.setEnabled(m_eventTrigRButton.isSelected());
                m_backwardRButton.setEnabled(m_eventTrigRButton.isSelected());
                m_centralRButton.setEnabled(m_eventTrigRButton.isSelected());
                m_limitWindowCheckBox.setEnabled(m_eventTrigRButton.isSelected());
            }
        };

        m_eventTrigRButton.addActionListener(triggerListener);
        m_timeTrigRButton.addActionListener(triggerListener);
        m_limitWindowCheckBox.addActionListener(triggerListener);

        m_forwardRButton.doClick();
        m_eventTrigRButton.doClick();

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
        constraint.fill = GridBagConstraints.HORIZONTAL;

        /* Trigger sub-panel*/
        JPanel triggerPanel = new JPanel(new GridBagLayout());

        GridBagConstraints subConstraint = new GridBagConstraints();
        subConstraint.ipadx = 2;
        subConstraint.ipady = 5;
        subConstraint.insets = new Insets(2, 2, 2, 2);
        subConstraint.gridx = 1;
        subConstraint.gridy = 1;

        triggerPanel.add(m_eventTrigRButton, subConstraint);

        subConstraint.gridx++;
        triggerPanel.add(m_timeTrigRButton, subConstraint);

        panel.add(triggerPanel, constraint);

        /* Event sub-panel */
        JPanel eventPanel = new JPanel(new GridBagLayout());

        subConstraint.gridx = 1;
        subConstraint.gridy = 1;

        eventPanel.add(m_windowSizeLabel, subConstraint);

        subConstraint.gridx++;
        eventPanel.add(m_windowSizeSpinner, subConstraint);

        subConstraint.gridx--;
        subConstraint.gridy++;
        eventPanel.add(m_stepSizeLabel, subConstraint);

        subConstraint.gridx++;
        eventPanel.add(m_stepSizeSpinner, subConstraint);

        subConstraint.gridx--;
        subConstraint.gridy++;
        eventPanel.add(m_forwardRButton, subConstraint);

        subConstraint.gridx++;
        eventPanel.add(m_centralRButton, subConstraint);

        subConstraint.gridx++;
        eventPanel.add(m_backwardRButton, subConstraint);

        subConstraint.gridx -= 2;
        subConstraint.gridy++;
        eventPanel.add(m_limitWindowCheckBox, subConstraint);

        eventPanel.setBorder(BorderFactory.createTitledBorder("Event Triggered"));

        constraint.gridy++;
        panel.add(eventPanel, constraint);

        /* Time sub-panel */
        JPanel timePanel = new JPanel(new GridBagLayout());

        subConstraint.gridx = 1;
        subConstraint.gridy = 1;

        Component[] comp = m_columnSelector.getComponentPanel().getComponents();
        timePanel.add(comp[0], subConstraint);

        subConstraint.gridx++;
        subConstraint.fill = GridBagConstraints.HORIZONTAL;
        timePanel.add(comp[1], subConstraint);

        subConstraint.gridx--;
        subConstraint.gridy++;
        subConstraint.fill = GridBagConstraints.NONE;
        timePanel.add(m_windowTimeLabel, subConstraint);

        subConstraint.gridx++;
        subConstraint.fill = GridBagConstraints.HORIZONTAL;
        timePanel.add(m_timeWindow, subConstraint);

        subConstraint.gridx--;
        subConstraint.gridy++;
        subConstraint.fill = GridBagConstraints.NONE;
        timePanel.add(m_startTimeLabel, subConstraint);

        subConstraint.gridx++;
        subConstraint.fill = GridBagConstraints.HORIZONTAL;
        timePanel.add(m_startTime, subConstraint);

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

        m_windowSizeSpinner.setValue(config.getWindowSize());
        m_stepSizeSpinner.setValue(config.getStepSize());
        m_limitWindowCheckBox.setSelected(config.getLimitWindow());

        switch (config.getWindowDefinition()) {
            case FORWARD:
                m_forwardRButton.doClick();
                break;
            case BACKWARD:
                m_backwardRButton.doClick();
                break;
            default:
                m_centralRButton.doClick();
        }

        switch (config.getTrigger()) {
            case EVENT:
                m_eventTrigRButton.doClick();
                break;
            default:
                m_timeTrigRButton.doClick();
                m_startTime.setText(DurationPeriodFormatUtils.formatDurationShort(config.getStartDuration()));
                m_timeWindow.setText(DurationPeriodFormatUtils.formatDurationShort(config.getWindowDuration()));
        }

        m_columnSelector.loadSettingsFrom(settings, specs);
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        LoopStartWindowConfiguration config = new LoopStartWindowConfiguration();
        config.setWindowSize((Integer)m_windowSizeSpinner.getValue());
        config.setStepSize((Integer)m_stepSizeSpinner.getValue());
        config.setLimitWindow(m_limitWindowCheckBox.isSelected());

        if (m_forwardRButton.isSelected()) {
            config.setWindowDefinition(WindowDefinition.FORWARD);
        } else if (m_backwardRButton.isSelected()) {
            config.setWindowDefinition(WindowDefinition.BACKWARD);
        } else {
            config.setWindowDefinition(WindowDefinition.CENTRAL);
        }

        if (m_eventTrigRButton.isSelected()) {
            config.setTrigger(Trigger.EVENT);
        } else {
            config.setTrigger(Trigger.TIME);

            if (m_columnSelector == null || m_columnSelector.getSelectedAsSpec() == null) {
                throw new InvalidSettingsException("No valid column has been chosen");
            }

            try {
                Duration startDur = DurationPeriodFormatUtils.parseDuration(m_startTime.getText());
                config.setStartInterval(startDur);
            } catch (DateTimeParseException e) {
                throw new InvalidSettingsException("No valid start duration: " + m_startTime.getText());
            }

            try {
                Duration windowDur = DurationPeriodFormatUtils.parseDuration(m_timeWindow.getText());
                config.setWindowDuration(windowDur);
            } catch (DateTimeParseException e) {
                throw new InvalidSettingsException("No valid window duration: " + m_timeWindow.getText());
            }
        }

        config.saveSettingsTo(settings);
        m_columnSelector.saveSettingsTo(settings);
    }

    /**
     * @return settings model for column selection
     */
    static final SettingsModelString createColumnModel() {
        return new SettingsModelString("selectedTimeColumn", null);
    }

}
