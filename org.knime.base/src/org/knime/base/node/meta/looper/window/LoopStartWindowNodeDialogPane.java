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

import java.awt.FlowLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.ButtonGroup;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.SpinnerNumberModel;

import org.knime.base.node.meta.looper.window.LoopStartWindowConfiguration.Mode;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;

/**
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public class LoopStartWindowNodeDialogPane extends NodeDialogPane {

    private final JRadioButton m_tumblingButton;

    private final JRadioButton m_slidingWindow;

    private final JSpinner m_stepSizeSpinner;

    private final JSpinner m_windowSizeSpinner;

    /**
     *
     */
    public LoopStartWindowNodeDialogPane() {
        ButtonGroup bg = new ButtonGroup();
        m_tumblingButton = new JRadioButton("Tumbling");
        m_slidingWindow = new JRadioButton("Sliding");
        ActionListener al = new ActionListener() {
            /** {@inheritDoc} */
            @Override
            public void actionPerformed(final ActionEvent e) {
                onNewSelection();
            }
        };
        m_tumblingButton.addActionListener(al);
        m_slidingWindow.addActionListener(al);
        bg.add(m_tumblingButton);
        bg.add(m_slidingWindow);
        m_windowSizeSpinner = new JSpinner(new SpinnerNumberModel(10, 1, Integer.MAX_VALUE, 5));
        m_stepSizeSpinner = new JSpinner(new SpinnerNumberModel(10, 1, Integer.MAX_VALUE, 10));
        m_tumblingButton.doClick();
        initLayout();
    }

    /**
     *
     */
    private void initLayout() {
        JPanel panel = new JPanel(new GridLayout(0, 2));
        panel.add(getInFlowLayout(m_tumblingButton));
        panel.add(getInFlowLayout(m_stepSizeSpinner));
        panel.add(getInFlowLayout(m_slidingWindow));
        panel.add(getInFlowLayout(m_windowSizeSpinner));
        addTab("Configuration", panel);
    }

    /**
     * @param rowsPerChunkButton
     * @return
     */
    private JPanel getInFlowLayout(final JComponent... comps) {
        JPanel result = new JPanel(new FlowLayout(FlowLayout.LEFT));
        for (JComponent c : comps) {
            result.add(c);
        }
        return result;
    }

    /**
     *
     */
    private void onNewSelection() {
        boolean isRowCountPerChunk = m_tumblingButton.isSelected();
        m_windowSizeSpinner.setEnabled(!isRowCountPerChunk);
        m_stepSizeSpinner.setEnabled(isRowCountPerChunk);
    }

    /** {@inheritDoc} */
    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec[] specs)
        throws NotConfigurableException {
        LoopStartWindowConfiguration config = new LoopStartWindowConfiguration();
        config.loadSettingsInDialog(settings);
        m_windowSizeSpinner.setValue(config.getWindowSize());
        m_stepSizeSpinner.setValue(config.getStepSize());
        switch (config.getMode()) {
            case TUMBLING:
                m_tumblingButton.doClick();
                break;
            default:
                m_slidingWindow.doClick();
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        LoopStartWindowConfiguration config = new LoopStartWindowConfiguration();
        config.setWindowSize((Integer)m_windowSizeSpinner.getValue());
        config.setStepSize((Integer)m_stepSizeSpinner.getValue());
        if (m_tumblingButton.isSelected()) {
            config.setMode(Mode.TUMBLING);
        } else {
            config.setMode(Mode.SLIDING);
        }
        config.saveSettingsTo(settings);
    }

}
