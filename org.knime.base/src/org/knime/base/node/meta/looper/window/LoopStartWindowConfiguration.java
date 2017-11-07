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
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
final class LoopStartWindowConfiguration {

    /** Policy how to do the chunking. */
    enum WindowMode {
            /** Use tumbling for windowing. */
            TUMBLING,
            /** Use sliding for windowing. */
            SLIDING
    }

    /** Window definition */
    enum WindowDefinition {
            /** Point of interest in the center of window. */
            CENTRAL,
            /** Point of interest at the beginning of the window. */
            FORWARD,
            /** Point of interest at the end of the window. */
            BACKWARD
    }

    enum Trigger {
            /** Event triggered. */
            EVENT,
            /** Time triggered. */
            TIME
    }

    private WindowMode windowMode = WindowMode.TUMBLING;

    private WindowDefinition windowDefinition = WindowDefinition.FORWARD;

    private Trigger trigger = Trigger.EVENT;

    private int stepSize = 1;

    private int windowSize = 1;

    /** @return the window mode */
    WindowMode getWindowMode() {
        return windowMode;
    }

    /** @return the window definition */
    WindowDefinition getWindowDefinition() {
        return windowDefinition;
    }

    /** @return the trigger */
    Trigger getTrigger() {
        return trigger;
    }

    /**
     * @param mode the window mode to set
     * @throws InvalidSettingsException If argument is null.
     */
    void setWindowMode(final WindowMode mode) throws InvalidSettingsException {
        if (mode == null) {
            throw new InvalidSettingsException("Window mode must not be null");
        }

        windowMode = mode;
    }

    /**
     * @param definition the window definition to set
     * @throws InvalidSettingsException If argument is null.
     */
    void setWindowDefinition(final WindowDefinition definition) throws InvalidSettingsException {
        if (definition == null) {
            throw new InvalidSettingsException("Window definition must not be null");
        }

        windowDefinition = definition;
    }

    /**
     * @param trigger the trigger to set
     * @throws InvalidSettingsException If argument is null.
     */
    void setTrigger(final Trigger trigger) throws InvalidSettingsException {
        if (trigger == null) {
            throw new InvalidSettingsException("Trigger must not be null");
        }

        this.trigger = trigger;
    }

    /** @return the step size. */
    int getStepSize() {
        return stepSize;
    }

    /**
     * @param stepSize the stepSize to set
     * @throws InvalidSettingsException If argument &lt; 1
     */
    void setStepSize(final int stepSize) throws InvalidSettingsException {
        if (stepSize < 1) {
            throw new IllegalArgumentException("Step size must be at least 1: " + stepSize);
        }

        this.stepSize = stepSize;
    }

    /** @return the size of the window. */
    int getWindowSize() {
        return windowSize;
    }

    /**
     * @param windowSize the size of window to set
     * @throws InvalidSettingsException If argument &lt; 1
     */
    void setWindowSize(final int windowSize) throws InvalidSettingsException {
        if (windowSize < 1) {
            throw new IllegalArgumentException("Window size must be at least 1: " + windowSize);
        }

        this.windowSize = windowSize;
    }

    /**
     * Saves current settings to argument.
     *
     * @param settings To save to.
     */
    void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString("windowMode", windowMode.name());
        settings.addInt("stepSize", stepSize);
        settings.addInt("windowSize", windowSize);
        settings.addString("trigger", trigger.name());
        settings.addString("windowDefinition", windowDefinition.name());
    }

    /**
     * Load settings in model, fails if incomplete.
     *
     * @param settings To load from.
     * @throws InvalidSettingsException If invalid.
     */
    void loadSettingsInModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        String modeS = settings.getString("windowMode");

        if (modeS == null) {
            modeS = WindowMode.SLIDING.name();
        }

        try {
            setWindowMode(WindowMode.valueOf(modeS));
        } catch (IllegalArgumentException iae) {
            throw new InvalidSettingsException("Invalid window mode: " + modeS);
        }

        String defS = settings.getString("windowDefinition");

        if (defS == null) {
            defS = WindowDefinition.FORWARD.name();
        }

        try {
            setWindowDefinition(WindowDefinition.valueOf(defS));
        } catch (IllegalArgumentException iae) {
            throw new InvalidSettingsException("Invalid window definition: " + defS);
        }

        String triggerS = settings.getString("trigger");

        if (triggerS == null) {
            triggerS = Trigger.EVENT.name();
        }

        try {
            setTrigger(Trigger.valueOf(triggerS));
        } catch (IllegalArgumentException iae) {
            throw new InvalidSettingsException("Invalid trigger: " + trigger);
        }

        setStepSize(settings.getInt("stepSize"));
        setWindowSize(settings.getInt("windowSize"));
    }

    /**
     * Load settings in dialog, use default if invalid.
     *
     * @param settings To load from.
     */
    void loadSettingsInDialog(final NodeSettingsRO settings) {
        try {
            trigger = Trigger.valueOf(settings.getString("trigger", Trigger.EVENT.name()));
        } catch (IllegalStateException e) {
            trigger = Trigger.EVENT;
        }

        try {
            windowDefinition =
                WindowDefinition.valueOf(settings.getString("windowDefinition", WindowDefinition.FORWARD.name()));
        } catch (IllegalArgumentException iae) {
            windowDefinition = WindowDefinition.FORWARD;
        }

        try {
            windowMode = WindowMode.valueOf(settings.getString("windowMode", WindowMode.SLIDING.name()));
        } catch (IllegalArgumentException iae) {
            windowMode = WindowMode.SLIDING;
        }

        try {
            setStepSize(settings.getInt("stepSize", 1));
        } catch (InvalidSettingsException ise) {
            stepSize = 1;
        }

        try {
            setWindowSize(settings.getInt("windowSize", 1));
        } catch (InvalidSettingsException e) {
            windowSize = 1;
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        switch (windowMode) {
            case SLIDING:
                return "sliding mode (window size: " + windowSize + ", step size: " + stepSize + ")";
            case TUMBLING:
                return "tumbling mode (window size: " + windowSize + ", step size: " + stepSize + ")";
            default:
                assert false : "Uncovered case";
                return "unknown";
        }
    }

}
