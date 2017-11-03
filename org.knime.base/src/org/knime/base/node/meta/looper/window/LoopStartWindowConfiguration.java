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

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Configuration object to loop start chunking node.
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
final class LoopStartWindowConfiguration {

    /** Policy how to do the chunking. */
    enum Mode {
        /** Use tumbling for windowing. */
        TUMBLING,
        /** Use sliding for windowing. */
        SLIDING
    }

    private Mode m_mode = Mode.TUMBLING;
    private int m_stepSize = 1;
    private int m_windowSize = 1;

    /** @return the mode */
    Mode getMode() {
        return m_mode;
    }
    /** @param mode the mode to set
     * @throws InvalidSettingsException If argument is null. */
    void setMode(final Mode mode) throws InvalidSettingsException {
        if (mode == null) {
            throw new InvalidSettingsException("Mode must not be null");
        }
        m_mode = mode;
    }
    /** @return the stepSize */
    int getStepSize() {
        return m_stepSize;
    }
    /** @param stepSize the stepSize to set
     * @throws InvalidSettingsException If argument &lt; 1*/
    void setStepSize(final int stepSize)
        throws InvalidSettingsException {
        if (stepSize < 1) {
            throw new IllegalArgumentException("No of rows per chunk must "
                    + "be at least 1: " + stepSize);
        }
        m_stepSize = stepSize;
    }
    /** @return the windowSize */
    int getWindowSize() {
        return m_windowSize;
    }
    /** @param windowSize the windowSize to set
    * @throws InvalidSettingsException If argument &lt; 1*/
    void setWindowSize(final int windowSize) throws InvalidSettingsException {
        if (windowSize < 1) {
            throw new IllegalArgumentException("No of chunks must "
                    + "be at least 1: " + windowSize);
        }
        m_windowSize = windowSize;
    }

    /** Saves current settings to argument.
     * @param settings To save to. */
    void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString("mode", m_mode.name());
        settings.addInt("stepSize", m_stepSize);
        settings.addInt("windowSize", m_windowSize);
    }

    /** Load settings in model, fails if incomplete.
     * @param settings To load from.
     * @throws InvalidSettingsException If invalid.
     */
    void loadSettingsInModel(final NodeSettingsRO settings)
        throws InvalidSettingsException {
        String modeS = settings.getString("mode");
        if (modeS == null) {
            modeS = Mode.TUMBLING.name();
        }
        try {
            setMode(Mode.valueOf(modeS));
        } catch (IllegalArgumentException iae) {
            throw new InvalidSettingsException("Invalid mode: " + modeS);
        }
        setStepSize(settings.getInt("stepSize"));
        setWindowSize(settings.getInt("windowSize"));
    }

    /** Load settings in dialog, use default if invalid.
     * @param settings To load from.
     */
    void loadSettingsInDialog(final NodeSettingsRO settings) {
        String modeS = settings.getString("mode", Mode.TUMBLING.name());
        if (modeS == null) {
            modeS = Mode.TUMBLING.name();
        }
        try {
            m_mode = Mode.valueOf(modeS);
        } catch (IllegalArgumentException iae) {
            m_mode = Mode.TUMBLING;
        }
        try {
            setStepSize(settings.getInt("stepSize", 1));
        } catch (InvalidSettingsException ise) {
            // use default;
        }
        try {
            setWindowSize(settings.getInt("windowSize", 1));
        } catch (InvalidSettingsException e) {
            // use default;
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        switch (m_mode) {
        case SLIDING:
            return "sliding mode (window size: " + m_windowSize + ", step size: "+m_stepSize+")";
        case TUMBLING:
            return "tumbling mode (window size: " + m_windowSize + ", step size: "+m_stepSize+")";
        default:
            assert false : "Uncovered case";
            return "unknown";
        }
    }

}
