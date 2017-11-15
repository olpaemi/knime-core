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
 * ---------------------------------------------------------------------
 *
 * History
 *   02.09.2008 (thor): created
 */
package org.knime.base.node.meta.looper.window;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

import org.knime.base.node.meta.looper.window.LoopStartWindowConfiguration.Trigger;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.time.localdate.LocalDateCell;
import org.knime.core.data.time.localdatetime.LocalDateTimeCell;
import org.knime.core.data.time.localtime.LocalTimeCell;
import org.knime.core.data.time.zoneddatetime.ZonedDateTimeCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.LoopStartNodeTerminator;

/**
 * Loop start node that outputs a set of rows at a time. Used to implement a streaming (or chunking approach) where only
 * a set of rows is processed at a time
 *
 * @author Moritz Heine, KNIME GmbH, Konstanz, Germany
 */
final class LoopStartWindowNodeModel extends NodeModel implements LoopStartNodeTerminator {

    // Config used to get the settings
    private LoopStartWindowConfiguration m_windowConfig;

    // Input iterator
    private CloseableRowIterator m_rowIterator;

    // number of columns
    private int m_nColumns;

    // index of current row
    private long m_currRow;

    private long m_rowCount;

    // buffered rows used for overlapping
    private LinkedList<DataRow> m_bufferedRows;

    // Name of the chosen time column
    private String m_timeColumnName;

    // Next start of the window
    private Temporal m_nextStartTemporal;

    // Next end of the window
    private Temporal m_windowEndTemporal;

    // Used to check if table is sorted
    private Temporal m_prevTemporal;

    // To check if an overflow occurred concerning next starting temporal.
    private boolean m_lastWindow;

    // To ensure that warning message will be printed only once
    private boolean m_printedMissingWarning;

    /**
     * Creates a new model.
     */
    public LoopStartWindowNodeModel() {
        super(1, 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs) throws InvalidSettingsException {
        if (m_windowConfig == null) {
            m_windowConfig = new LoopStartWindowConfiguration();
            setWarningMessage("Using default: " + m_windowConfig);
        }

        DataTableSpec tableSpec = inSpecs[0];

        if (m_windowConfig.getTrigger() == Trigger.TIME && !tableSpec.containsName(m_timeColumnName)) {
            throw new InvalidSettingsException(
                "Selected time column '" + m_timeColumnName + "' does not exist in input table.");
        }

        return inSpecs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws Exception {
        BufferedDataTable table = inData[0];
        m_rowCount = table.size();

        if (m_currRow == 0) {
            m_rowIterator = table.iterator();
            m_bufferedRows = new LinkedList<>();

            m_nColumns = table.getSpec().getNumColumns();
        }

        switch (m_windowConfig.getWindowDefinition()) {
            case BACKWARD:
                if (m_windowConfig.getTrigger().equals(Trigger.EVENT)) {
                    return executeBackward(table, exec);
                }

                return executeTemporalBackward(table, exec);

            case CENTRAL:
                if (m_windowConfig.getTrigger().equals(Trigger.EVENT)) {
                    return executeCentral(table, exec);
                }

                return executeTemporalCentral(table, exec);

            case FORWARD:
                if (m_windowConfig.getTrigger().equals(Trigger.EVENT)) {
                    return executeForward(table, exec);
                }

                return executeTemporalForward(table, exec);

            default:
                return executeForward(table, exec);

        }
    }

    /**
     * Computes the next window that shall be returned for time triggered events using forward windowing.
     *
     * @param table that holds the data.
     * @param exec context of the execution.
     * @return Next window.
     */
    private BufferedDataTable[] executeTemporalForward(final BufferedDataTable table, final ExecutionContext exec) {
        int column = table.getDataTableSpec().findColumnIndex(m_timeColumnName);
        Duration startInterval = m_windowConfig.getStartDuration();
        Duration windowDuration = m_windowConfig.getWindowDuration();
        Temporal windowEnd = null;

        /* To check if an overflow occurred concerning the current window */
        boolean overflow = false;
        // To check if an overflow occurred concerning next starting temporal.
        m_lastWindow = false;

        /* Compute end duration of window and beginning of next duration*/
        if (m_nextStartTemporal == null && m_rowIterator.hasNext()) {
            DataRow first = m_rowIterator.next();

            /* Check if column only consists of missing values. */
            while (first.getCell(column).isMissing() && m_rowIterator.hasNext()) {
                first = m_rowIterator.next();

                printMissingWarning();
            }

            if (first.getCell(column).isMissing()) {
                throw new IllegalArgumentException("Chosen column only contains missing values.");
            }

            Temporal firstStart = getTemporal(first.getCell(column));

            m_prevTemporal = firstStart;

            m_nextStartTemporal = firstStart.plus(startInterval);

            /* Check if the next starting temporal lies beyond the maximum temporal value. */
            if (compareTemporal(m_nextStartTemporal, firstStart) <= 0) {
                m_lastWindow = true;
            }

            /* Checks if window overflow occurs. */
            Temporal temp = firstStart.plus(windowDuration);
            if (compareTemporal(temp, firstStart) <= 0) {
                overflow = true;
            } else {
                windowEnd = temp;
            }

            m_bufferedRows.add(first);
        } else {
            m_prevTemporal = getTemporal(m_bufferedRows.getFirst().getCell(column));

            /* Checks if temporal overflow occurs. */
            Temporal temp = m_nextStartTemporal.plus(windowDuration);
            if (compareTemporal(temp, m_nextStartTemporal.minus(startInterval)) <= 0) {
                overflow = true;
            } else {
                windowEnd = temp;
                temp = m_nextStartTemporal.plus(startInterval);
                /* Check if the next starting temporal lies beyond the maximum temporal value. */
                if (compareTemporal(temp, m_nextStartTemporal) <= 0) {
                    m_lastWindow = true;
                } else {
                    m_nextStartTemporal = temp;
                }
            }
        }

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        Iterator<DataRow> bufferedIterator = m_bufferedRows.iterator();

        boolean allBufferedRowsInWindow = true;

        /* Add buffered rows. */
        while (bufferedIterator.hasNext()) {
            DataRow row = bufferedIterator.next();

            Temporal temp = getTemporal(row.getCell(column));

            /* Checks if all buffered rows are in the specified window. */
            if (!overflow && compareTemporal(temp, windowEnd) > 0) {
                allBufferedRowsInWindow = false;
                break;
            }

            container.addRowToTable(row);
            m_currRow++;

            if (overflow || m_lastWindow
                || compareTemporal(getTemporal(row.getCell(column)), m_nextStartTemporal) < 0) {
                bufferedIterator.remove();
            }
        }

        boolean lastEntryMissing = false;

        /* Add newly read rows. */
        while (m_rowIterator.hasNext() && allBufferedRowsInWindow) {
            DataRow row = m_rowIterator.next();

            if (row.getCell(column).isMissing()) {
                printMissingWarning();
                lastEntryMissing = true;
                continue;
            }

            lastEntryMissing = false;

            Temporal currTemporal = getTemporal(row.getCell(column));

            /* Check if table is sorted in non-descending order according to temporal column. */
            if (compareTemporal(currTemporal, m_prevTemporal) < 0) {
                throw new IllegalStateException("Table not in ascending order concerning chosen temporal column.");
            }

            m_prevTemporal = currTemporal;

            /* Add rows for next window into the buffer. */
            if (compareTemporal(currTemporal, m_nextStartTemporal) >= 0 && !overflow && !m_lastWindow) {
                m_bufferedRows.add(row);
            }

            /* Add row to current output. */
            if (overflow || compareTemporal(currTemporal, windowEnd) <= 0) {
                container.addRowToTable(row);

                /* The last entry has been in the current window, thus it is the last one. */
                if (!m_rowIterator.hasNext()) {
                    m_lastWindow = true;
                }

                m_currRow++;
            } else {
                break;
            }
        }

        /* Checks if the last row we saw had a missing value. If this is the case the current window is the last window. */
        if (lastEntryMissing) {
            m_lastWindow = true;
        }

        /* Find next entry that lies in a following window. */
        DataRow row = null;

        /* Close iterator if last window has been filled. */
        if (m_lastWindow) {
            m_rowIterator.close();
        } else if (!allBufferedRowsInWindow) {
            row = m_bufferedRows.remove();
        } else if (m_bufferedRows.size() == 0 && m_rowIterator.hasNext()) {
            row = m_rowIterator.next();

            while (row.getCell(column).isMissing() && m_rowIterator.hasNext()) {
                row = m_rowIterator.next();

                printMissingWarning();
            }

            if (row.getCell(column).isMissing()) {
                row = null;

                printMissingWarning();
            }
        } else if (!overflow && !m_bufferedRows.isEmpty()) {
            /* Checks if the next buffered row lies within the given window */
            if (compareTemporal(windowEnd.plus(startInterval), windowEnd) > 0) {
                Temporal temp = getTemporal(m_bufferedRows.getFirst().getCell(column));

                if (compareTemporal(temp, windowEnd.plus(startInterval)) >= 0) {
                    row = m_bufferedRows.removeFirst();
                }
            }
        }

        skipTemporalWindows(row, column, startInterval, windowDuration);

        container.close();

        return new BufferedDataTable[]{container.getTable()};
    }

    /**
     * Prints a warning concerning missing values.
     */
    private void printMissingWarning() {
        if (!m_printedMissingWarning) {
            m_printedMissingWarning = true;
            getLogger().warn("Detected missing values for specified column; rows have been skipped.");
        }

    }

    /**
     * Computes the next window that shall be returned for time triggered events using backward windowing.
     *
     * @param table that holds the data.
     * @param exec context of the execution.
     * @return Next window.
     */
    private BufferedDataTable[] executeTemporalBackward(final BufferedDataTable table, final ExecutionContext exec) {
        int column = table.getDataTableSpec().findColumnIndex(m_timeColumnName);
        Duration startInterval = m_windowConfig.getStartDuration();
        Duration windowDuration = m_windowConfig.getWindowDuration();

        /* To check if an overflow occurred concerning the current window */
        boolean overflow = false;
        // To check if an overflow occurred concerning next starting temporal.
        m_lastWindow = false;

        /* Compute end duration of window and beginning of next duration*/
        if (m_nextStartTemporal == null && m_rowIterator.hasNext()) {
            DataRow first = m_rowIterator.next();

            /* Check if column only consists of missing values. */
            while (first.getCell(column).isMissing() && m_rowIterator.hasNext()) {
                first = m_rowIterator.next();

                printMissingWarning();
            }

            if (first.getCell(column).isMissing()) {
                throw new IllegalArgumentException("Chosen column only contains missing values.");
            }

            /* First entry is the end of the window. Compute next starting point by adding start interval minus the size of the window. */
            Temporal firstStart = getTemporal(first.getCell(column));
            m_windowEndTemporal = firstStart;

            m_prevTemporal = firstStart;

            m_bufferedRows.add(first);

            Temporal tempNextEnd = m_windowEndTemporal.plus(startInterval);

            /* Check if the current window is the last window. */
            if (compareTemporal(tempNextEnd, m_windowEndTemporal) <= 0) {
                m_lastWindow = true;
            } else {
                m_nextStartTemporal = tempNextEnd.minus(windowDuration);

                if (compareTemporal(m_nextStartTemporal, tempNextEnd) >= 0) {
                    m_nextStartTemporal = getMin(tempNextEnd);
                }
            }

        } else {
            Temporal tempNextEnd = m_windowEndTemporal.plus(startInterval);

            /* Check if the current window is the last window. */
            if (compareTemporal(tempNextEnd, m_windowEndTemporal) <= 0) {
                m_lastWindow = true;
            } else {
                m_nextStartTemporal = tempNextEnd.minus(windowDuration);

                if (compareTemporal(m_nextStartTemporal, tempNextEnd) >= 0) {
                    m_nextStartTemporal = getMin(tempNextEnd);
                }
            }

            m_windowEndTemporal = tempNextEnd;
        }

        Temporal tempNextEnd = m_windowEndTemporal.plus(startInterval);

        /* Check if the current window is the last window. */
        if (compareTemporal(tempNextEnd, m_windowEndTemporal) <= 0) {
            m_lastWindow = true;
        } else {
            m_nextStartTemporal = tempNextEnd.minus(windowDuration);

            if (compareTemporal(m_nextStartTemporal, tempNextEnd) >= 0) {
                m_nextStartTemporal = getMin(tempNextEnd);
            }
        }

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        Iterator<DataRow> bufferedIterator = m_bufferedRows.iterator();

        boolean allBufferedRowsInWindow = true;

        /* Add buffered rows. */
        while (bufferedIterator.hasNext()) {
            DataRow row = bufferedIterator.next();

            Temporal temp = getTemporal(row.getCell(column));

            /* Checks if all buffered rows are in the specified window. */
            if (!overflow && compareTemporal(temp, m_windowEndTemporal) > 0) {
                allBufferedRowsInWindow = false;
                break;
            }

            container.addRowToTable(row);
            m_currRow++;

            if (overflow || m_lastWindow
                || compareTemporal(getTemporal(row.getCell(column)), m_nextStartTemporal) < 0) {
                bufferedIterator.remove();
            }
        }

        boolean lastEntryMissing = false;

        /* Add newly read rows. */
        while (m_rowIterator.hasNext() && allBufferedRowsInWindow) {
            DataRow row = m_rowIterator.next();

            if (row.getCell(column).isMissing()) {
                printMissingWarning();
                lastEntryMissing = true;
                continue;
            }

            lastEntryMissing = false;

            Temporal currTemporal = getTemporal(row.getCell(column));

            /* Check if table is sorted in non-descending order according to temporal column. */
            if (compareTemporal(currTemporal, m_prevTemporal) < 0) {
                throw new IllegalStateException("Table not in ascending order concerning chosen temporal column.");
            }

            m_prevTemporal = currTemporal;

            /* Add rows for next window into the buffer. */
            if (compareTemporal(currTemporal, m_nextStartTemporal) >= 0 && !overflow && !m_lastWindow) {
                m_bufferedRows.add(row);
            }

            /* Add row to current output. */
            if (overflow || compareTemporal(currTemporal, m_windowEndTemporal) <= 0) {
                container.addRowToTable(row);

                /* The last entry has been in the current window, thus it is the last one. */
                if (!m_rowIterator.hasNext()) {
                    m_lastWindow = true;
                }

                m_currRow++;
            } else {
                break;
            }
        }

        /* Checks if the last row we saw had a missing value. If this is the case the current window is the last window. */
        if (lastEntryMissing) {
            m_lastWindow = true;
        }

        /* Find next entry that lies in a following window. */
        DataRow row = null;

        /* Close iterator if last window has been filled. */
        if (m_lastWindow) {
            m_rowIterator.close();
        } else if (!allBufferedRowsInWindow) {
            row = m_bufferedRows.remove();
        } else if (m_bufferedRows.size() == 0 && m_rowIterator.hasNext()) {
            row = m_rowIterator.next();

            while (row.getCell(column).isMissing() && m_rowIterator.hasNext()) {
                row = m_rowIterator.next();

                printMissingWarning();
            }

            if (row.getCell(column).isMissing()) {
                row = null;

                printMissingWarning();
            }
        } else if (!overflow && !m_bufferedRows.isEmpty()) {
            /* Checks if the next buffered row lies within the given window */
            if (compareTemporal(m_windowEndTemporal.plus(startInterval), m_windowEndTemporal) > 0) {
                Temporal temp = getTemporal(m_bufferedRows.getFirst().getCell(column));

                if (compareTemporal(temp, m_windowEndTemporal.plus(startInterval)) >= 0) {
                    row = m_bufferedRows.removeFirst();
                }
            }
        }

        skipTemporalWindows(row, column, startInterval, windowDuration);

        container.close();

        return new BufferedDataTable[]{container.getTable()};
    }

    /**
     * Computes the next window that shall be returned for time triggered events using central windowing.
     *
     * @param table that holds the data.
     * @param exec context of the execution.
     * @return Next window.
     */
    private BufferedDataTable[] executeTemporalCentral(final BufferedDataTable table, final ExecutionContext exec) {
        int column = table.getDataTableSpec().findColumnIndex(m_timeColumnName);
        Duration startInterval = m_windowConfig.getStartDuration();
        Duration windowDuration = m_windowConfig.getWindowDuration();

        /* To check if an overflow occurred concerning the current window */
        boolean overflow = false;
        // To check if an overflow occurred concerning next starting temporal.
        m_lastWindow = false;

        /* Compute end duration of window and beginning of next duration*/
        if (m_nextStartTemporal == null && m_rowIterator.hasNext()) {
            DataRow first = m_rowIterator.next();

            /* Check if column only consists of missing values. */
            while (first.getCell(column).isMissing() && m_rowIterator.hasNext()) {
                first = m_rowIterator.next();

                printMissingWarning();
            }

            if (first.getCell(column).isMissing()) {
                throw new IllegalArgumentException("Chosen column only contains missing values.");
            }

            Temporal firstStart = getTemporal(first.getCell(column));

            m_prevTemporal = firstStart;

            m_windowEndTemporal = firstStart.plus(windowDuration.dividedBy(2));
            m_nextStartTemporal = firstStart.minus(windowDuration.dividedBy(2));

            if (compareTemporal(m_windowEndTemporal, firstStart) <= 0) {
                overflow = true;
                m_lastWindow = true;
            } else {

                /* TODO: check setting nextStart as it wasnt set and yielded null pointer. */
                Temporal tempNextEnd = m_windowEndTemporal.plus(startInterval);
                Temporal tempNextStart = tempNextEnd.minus(windowDuration);

                /* Check if the current window is the last window. */
                if (compareTemporal(tempNextStart, m_nextStartTemporal) <= 0) {
                    m_lastWindow = true;
                } else if (compareTemporal(tempNextEnd, m_windowEndTemporal) > 0
                    && compareTemporal(tempNextStart, tempNextEnd) >= 0) {
                    /* Underflow occurred; set next start to minimum. */
                    m_nextStartTemporal = getMin(tempNextStart);
                } else {
                    m_nextStartTemporal = tempNextStart;
                }
            }

            m_bufferedRows.add(first);
        } else {
            m_prevTemporal = getTemporal(m_bufferedRows.getFirst().getCell(column));

            Temporal tempEnd = m_windowEndTemporal.plus(startInterval);

            /* Check for overflow of the window. */
            if (compareTemporal(tempEnd, m_windowEndTemporal) <= 0) {
                overflow = true;
            } else {
                m_windowEndTemporal = tempEnd;

                Temporal tempNextEnd = m_windowEndTemporal.plus(startInterval);
                Temporal tempNextStart = tempNextEnd.minus(windowDuration);

                /* Check if the current window is the last window. */
                if (compareTemporal(tempNextStart, m_nextStartTemporal) <= 0) {
                    m_lastWindow = true;
                } else if (compareTemporal(tempNextEnd, m_windowEndTemporal) > 0
                    && compareTemporal(tempNextStart, tempNextEnd) >= 0) {
                    /* Underflow occurred; set next start to minimum. */
                    m_nextStartTemporal = getMin(tempNextStart);
                } else {
                    m_nextStartTemporal = tempNextStart;
                }
            }
        }

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        Iterator<DataRow> bufferedIterator = m_bufferedRows.iterator();

        boolean allBufferedRowsInWindow = true;

        /* Add buffered rows. */
        while (bufferedIterator.hasNext()) {
            DataRow row = bufferedIterator.next();

            Temporal temp = getTemporal(row.getCell(column));

            /* Checks if all buffered rows are in the specified window. */
            if (!overflow && compareTemporal(temp, m_windowEndTemporal) > 0) {
                allBufferedRowsInWindow = false;
                break;
            }

            container.addRowToTable(row);
            m_currRow++;

            if (overflow || m_lastWindow
                || compareTemporal(getTemporal(row.getCell(column)), m_nextStartTemporal) < 0) {
                bufferedIterator.remove();
            }
        }

        boolean lastEntryMissing = false;

        /* Add newly read rows. */
        while (m_rowIterator.hasNext() && allBufferedRowsInWindow) {
            DataRow row = m_rowIterator.next();

            if (row.getCell(column).isMissing()) {
                printMissingWarning();
                lastEntryMissing = true;
                continue;
            }

            lastEntryMissing = false;

            Temporal currTemporal = getTemporal(row.getCell(column));

            /* Check if table is sorted in non-descending order according to temporal column. */
            if (compareTemporal(currTemporal, m_prevTemporal) < 0) {
                throw new IllegalStateException("Table not in ascending order concerning chosen temporal column.");
            }

            m_prevTemporal = currTemporal;

            /* Add rows for next window into the buffer. */
            if (compareTemporal(currTemporal, m_nextStartTemporal) >= 0 && !overflow && !m_lastWindow) {
                m_bufferedRows.add(row);
            }

            /* Add row to current output. */
            if (overflow || compareTemporal(currTemporal, m_windowEndTemporal) <= 0) {
                container.addRowToTable(row);

                /* The last entry has been in the current window, thus it is the last one. */
                if (!m_rowIterator.hasNext()) {
                    m_lastWindow = true;
                }

                m_currRow++;
            } else {
                break;
            }
        }

        /* Checks if the last row we saw had a missing value. If this is the case the current window is the last window. */
        if (lastEntryMissing) {
            m_lastWindow = true;
        }

        /* Find next entry that lies in a following window. */
        DataRow row = null;

        /* Close iterator if last window has been filled. */
        if (m_lastWindow) {
            m_rowIterator.close();
        } else if (!allBufferedRowsInWindow) {
            row = m_bufferedRows.remove();
        } else if (m_bufferedRows.size() == 0 && m_rowIterator.hasNext()) {
            row = m_rowIterator.next();

            while (row.getCell(column).isMissing() && m_rowIterator.hasNext()) {
                row = m_rowIterator.next();

                printMissingWarning();
            }

            if (row.getCell(column).isMissing()) {
                row = null;

                printMissingWarning();
            }
        } else if (!overflow && !m_bufferedRows.isEmpty()) {
            /* Checks if the next buffered row lies within the given window */
            if (compareTemporal(m_windowEndTemporal.plus(startInterval), m_windowEndTemporal) > 0) {
                Temporal temp = getTemporal(m_bufferedRows.getFirst().getCell(column));

                if (compareTemporal(temp, m_windowEndTemporal.plus(startInterval)) >= 0) {
                    row = m_bufferedRows.removeFirst();
                }
            }
        }

        skipTemporalWindows(row, column, startInterval, windowDuration);

        container.close();

        return new BufferedDataTable[]{container.getTable()};
    }

    /**
     * @param firstStart
     * @return
     */
    private Temporal getMin(final Temporal t1) {
        if (t1 instanceof LocalTime) {
            return LocalTime.MIN;
        } else if (t1 instanceof LocalDateTime) {
            return LocalDateTime.MIN;
        } else if (t1 instanceof LocalDate) {
            return LocalDate.MIN;
        }

        throw new IllegalArgumentException(
            "Data must be of type LocalDate, LocalDateTime, LocalTime, or ZonedDateTime");
    }

    /**
     * Skips the window for temporal datatypes until we obtain a window containing at least one row.
     *
     * @param row that is currently considered.
     * @param column index of the time column.
     * @param startInterval starting interval of the windows.
     * @param windowDuration duration of the window.
     */
    private void skipTemporalWindows(DataRow row, final int column, final Duration startInterval,
        final Duration windowDuration) {
        while (row != null) {
            /* Check if current row lies beyond next starting temporal. */
            while (compareTemporal(getTemporal(row.getCell(column)), m_nextStartTemporal) < 0
                && m_rowIterator.hasNext()) {
                DataRow temp = m_rowIterator.next();

                if (temp.getCell(column).isMissing()) {
                    printMissingWarning();

                    continue;
                }

                row = temp;
            }

            /* Checks if current row lies within next temporal window or if overflow of window occurs */
            if (compareTemporal(getTemporal(row.getCell(column)), m_nextStartTemporal.plus(windowDuration)) <= 0
                || compareTemporal(m_nextStartTemporal.plus(windowDuration), m_nextStartTemporal) < 0) {
                m_bufferedRows.addFirst(row);
                break;
            } else if (compareTemporal(getTemporal(row.getCell(column)), m_nextStartTemporal) < 0
                && !m_rowIterator.hasNext()) {
                /* There are no more rows that could lie within an upcoming window. */
                break;
            }

            /* If next row lies beyond the defined next window move it until the rows lies within an upcoming window or the window passed said row. */
            Temporal nextTemporalStart = m_nextStartTemporal.plus(startInterval);

            /* Check for overflow of the next starting interval. */
            if (compareTemporal(nextTemporalStart, m_nextStartTemporal) <= 0) {
                m_rowIterator.close();
                break;
            } else {
                m_nextStartTemporal = nextTemporalStart;
            }
        }

    }

    /**
     * Compares the temporal.
     *
     * @param t1 first temporal
     * @param t2 second temporal
     * @return the comparator value, negative if less, positive if greater
     */
    private int compareTemporal(final Temporal t1, final Temporal t2) {
        if (t1 instanceof LocalTime) {
            return ((LocalTime)t1).compareTo((LocalTime)t2);
        } else if (t1 instanceof LocalDateTime) {
            return ((LocalDateTime)t1).compareTo((LocalDateTime)t2);
        } else if (t1 instanceof LocalDate) {
            return ((LocalDate)t1).compareTo((LocalDate)t2);
        } else if (t1 instanceof ZonedDateTime) {
            return ((ZonedDateTime)t1).compareTo((ZonedDateTime)t2);
        }

        throw new IllegalArgumentException(
            "Data must be of type LocalDate, LocalDateTime, LocalTime, or ZonedDateTime");
    }

    /**
     * Returns the content of the given DataCell.
     *
     * @param cell which holds the content
     * @return temporal object of the cell, null if the DataCell does not contain a temporal object.
     */
    private Temporal getTemporal(final DataCell cell) {
        if (cell instanceof LocalTimeCell) {
            return ((LocalTimeCell)cell).getLocalTime();
        } else if (cell instanceof LocalDateCell) {
            LocalDate date = ((LocalDateCell)cell).getLocalDate();
            LocalDateTime dateTime = LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0);
            return dateTime;
        } else if (cell instanceof LocalDateTimeCell) {
            return ((LocalDateTimeCell)cell).getLocalDateTime();
        } else if (cell instanceof ZonedDateTimeCell) {
            return ((ZonedDateTimeCell)cell).getZonedDateTime();
        }

        return null;
    }

    /**
     * Executes backward windowing.
     *
     * @param table input data
     * @param exec ExecutionContext
     * @return BufferedDataTable containing the current loop.
     */
    private BufferedDataTable[] executeBackward(final BufferedDataTable table, final ExecutionContext exec) {
        int windowSize = m_windowConfig.getWindowSize();
        int stepSize = m_windowConfig.getStepSize();
        int currRowCount = 0;

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());

        /* Jump to next following row if step size is greater than the window size. */
        if (stepSize > windowSize && m_currRow > 0) {
            int diff = stepSize - windowSize;

            while (diff > 0 && m_rowIterator.hasNext()) {
                m_rowIterator.next();
                diff--;
            }
        }

        /* If window is limited, i.e. no missing rows shall be inserted, move the window until there are no missing rows. */
        if (m_windowConfig.getLimitWindow() && m_currRow < windowSize) {
            /* windowSize-1 are the number of rows we have in front of the considered row. */
            long bufferStart = -(windowSize - 1);
            long nextRow = m_currRow;

            while (bufferStart < 0) {
                bufferStart += stepSize;
                nextRow += stepSize;
            }

            while (m_currRow < bufferStart && m_rowIterator.hasNext()) {
                m_rowIterator.next();
                m_currRow++;
            }

            while (m_currRow < nextRow && m_rowIterator.hasNext()) {
                m_bufferedRows.add(m_rowIterator.next());
                m_currRow++;
            }
        }

        /* Add missing preceding rows to fill up the window at the beginning of the loop. */
        while (container.size() < windowSize - (m_currRow + 1)) {
            container.addRowToTable(new MissingRow(m_nColumns));
            currRowCount++;
        }

        /* Add buffered rows that overlap. */
        Iterator<DataRow> bufferedIterator = m_bufferedRows.iterator();

        while (bufferedIterator.hasNext()) {
            container.addRowToTable(bufferedIterator.next());

            if (currRowCount < stepSize) {
                bufferedIterator.remove();
            }

            currRowCount++;
        }

        /* Add newly read rows. */
        for (; container.size() < windowSize && m_rowIterator.hasNext(); currRowCount++) {
            DataRow dRow = m_rowIterator.next();

            if (currRowCount >= stepSize) {
                m_bufferedRows.add(dRow);
            }

            container.addRowToTable(dRow);
        }

        m_currRow += stepSize;

        container.close();

        return new BufferedDataTable[]{container.getTable()};
    }

    /**
     * Executes central windowing
     *
     * @param table input data
     * @param exec ExecutionContext
     * @return BufferedDataTable containing the current loop.
     */
    private BufferedDataTable[] executeCentral(final BufferedDataTable table, final ExecutionContext exec) {
        int windowSize = m_windowConfig.getWindowSize();
        int stepSize = m_windowConfig.getStepSize();
        int currRowCount = 0;

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());

        /* Jump to next following row if step size is greater than the window size.*/
        if (stepSize > windowSize && m_currRow > 0) {
            int diff = stepSize - windowSize;

            while (diff > 0 && m_rowIterator.hasNext()) {
                m_rowIterator.next();
                diff--;
            }
        }

        /* If window is limited, i.e. no missing rows shall be inserted, move the window until there are no missing rows. */
        if (m_windowConfig.getLimitWindow() && m_currRow < Math.floorDiv(windowSize, 2)) {
            long bufferStart = -Math.floorDiv(windowSize, 2);
            long nextRow = m_currRow;

            while (bufferStart < 0) {
                bufferStart += stepSize;
                nextRow += stepSize;
            }

            while (m_currRow < bufferStart && m_rowIterator.hasNext()) {
                m_rowIterator.next();
                m_currRow++;
            }

            while (m_currRow < nextRow && m_rowIterator.hasNext()) {
                m_bufferedRows.add(m_rowIterator.next());
                m_currRow++;
            }
        }

        /* Fill missing preceding rows with missing values. Only needed at the start of*/
        while (container.size() < Math.floorDiv(windowSize, 2) - (m_currRow)) {
            container.addRowToTable(new MissingRow(m_nColumns));
            currRowCount++;
        }

        Iterator<DataRow> bufferedIterator = m_bufferedRows.iterator();

        /* Add buffered rows that overlap. */
        while (bufferedIterator.hasNext()) {
            container.addRowToTable(bufferedIterator.next());

            if (currRowCount < stepSize) {
                bufferedIterator.remove();
            }

            currRowCount++;
        }

        /* Add newly read rows. */
        for (; container.size() < windowSize && m_rowIterator.hasNext(); currRowCount++) {
            DataRow dRow = m_rowIterator.next();

            if (currRowCount >= stepSize) {
                m_bufferedRows.add(dRow);
            }

            container.addRowToTable(dRow);
        }

        /* Add missing rows to fill up the window. */
        while (container.size() < windowSize) {
            container.addRowToTable(new MissingRow(m_nColumns));
        }

        m_currRow += stepSize;

        container.close();

        return new BufferedDataTable[]{container.getTable()};
    }

    /**
     * Executes forward windowing
     *
     * @param table input data
     * @param exec ExecutionContext
     * @return BufferedDataTable containing the current loop.
     */
    private BufferedDataTable[] executeForward(final BufferedDataTable table, final ExecutionContext exec) {
        int windowSize = m_windowConfig.getWindowSize();
        int stepSize = m_windowConfig.getStepSize();
        int currRowCount = 0;

        /* Jump to next following row if step size is greater than the window size.*/
        if (stepSize > windowSize && m_currRow > 0) {
            int diff = stepSize - windowSize;

            while (diff > 0 && m_rowIterator.hasNext()) {
                m_rowIterator.next();
                diff--;
            }
        }

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        Iterator<DataRow> bufferedIterator = m_bufferedRows.iterator();

        /* Add buffered rows that overlap. */
        while (bufferedIterator.hasNext()) {
            container.addRowToTable(bufferedIterator.next());

            if (currRowCount < stepSize) {
                bufferedIterator.remove();
            }

            currRowCount++;
        }

        /* Add newly read rows. */
        for (; container.size() < windowSize && m_rowIterator.hasNext(); currRowCount++) {
            DataRow dRow = m_rowIterator.next();

            if (currRowCount >= stepSize) {
                m_bufferedRows.add(dRow);
            }

            container.addRowToTable(dRow);
        }

        /* Add missing rows to fill up the window. */
        while (container.size() < windowSize) {
            container.addRowToTable(new MissingRow(m_nColumns));
        }

        m_currRow += stepSize;

        container.close();

        return new BufferedDataTable[]{container.getTable()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        m_currRow = 0;
        MissingRow.rowCounter = 0;
        m_lastWindow = false;
        m_printedMissingWarning = false;

        if (m_rowIterator != null) {
            m_rowIterator.close();
        }

        m_rowIterator = null;
        m_nextStartTemporal = null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean terminateLoop() {
        if (m_windowConfig.getTrigger().equals(Trigger.EVENT)) {
            /* If we limit the window to fit in the table we might terminate earlier. */
            if (m_windowConfig.getLimitWindow()) {
                /* Given window is too large. */
                if (m_rowCount < m_windowConfig.getWindowSize()) {
                    return true;
                }

                switch (m_windowConfig.getWindowDefinition()) {
                    case FORWARD:
                        return m_rowCount - m_currRow < m_windowConfig.getWindowSize();
                    case BACKWARD:
                        return m_currRow >= m_rowCount;
                    case CENTRAL:
                        return m_rowCount - m_currRow < Math.ceil(((double)m_windowConfig.getWindowSize()) / 2) - 1;
                    default:
                        return true;
                }
            }

            return m_currRow >= m_rowCount;
        }

        /* TODO: check what happens if theres an entry in bufferedRows but we just used the last entry. */
        return m_lastWindow || !m_rowIterator.hasNext() && (m_bufferedRows == null || m_bufferedRows.isEmpty());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_windowConfig != null) {
            m_windowConfig.saveSettingsTo(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        new LoopStartWindowConfiguration().loadSettingsInModel(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        LoopStartWindowConfiguration config = new LoopStartWindowConfiguration();
        config.loadSettingsInModel(settings);
        m_windowConfig = config;

        SettingsModelString settingsModel = LoopStartWindowNodeDialog.createColumnModel();
        settingsModel.loadSettingsFrom(settings);
        m_timeColumnName = settingsModel.getStringValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // no internals to load
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // no internals to save
    }

    /**
     * An InputRow with solely missing data cells, needed for different window definitions. Copied:
     * org.knime.base.node.preproc.joiner.DataHiliteOutputContainer.Missing
     *
     * @author Heiko Hofer
     */
    static class MissingRow implements DataRow {
        private DataCell[] m_cells;

        private static int rowCounter = 0;

        private static String rowName = "LSW_Missing_Row";

        /**
         * @param numCells The number of cells in the {@link DataRow}
         */
        public MissingRow(final int numCells) {
            m_cells = new DataCell[numCells];
            for (int i = 0; i < numCells; i++) {
                m_cells[i] = DataType.getMissingCell();
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public RowKey getKey() {
            return new RowKey(rowName + (rowCounter++));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DataCell getCell(final int index) {
            return m_cells[index];
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int getNumCells() {
            return m_cells.length;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Iterator<DataCell> iterator() {
            return Arrays.asList(m_cells).iterator();
        }
    }
}
