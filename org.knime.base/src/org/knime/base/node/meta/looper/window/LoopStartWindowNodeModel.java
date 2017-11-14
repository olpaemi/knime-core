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
import org.knime.core.data.time.duration.DurationCell;
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
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public class LoopStartWindowNodeModel extends NodeModel implements LoopStartNodeTerminator {

    private LoopStartWindowConfiguration windowConfig;

    // loop invariants
    // private BufferedDataTable m_table;

    private CloseableRowIterator rowIterator;

    // loop variants
    // private int m_iteration;

    // number of columns
    private int nColumns;

    // index of current row
    private long currRow;

    private long rowCount;

    // buffered rows used for overlapping
    private LinkedList<DataRow> bufferedRows;

    private String timeColumnName;

    private Duration nextStartDuration;

    private Temporal nextStartTemporal;

    // Used to check if table is sorted
    private Duration prevDuration;

    // Used to check if table is sorted
    private Temporal prevTemporal;

    private boolean lastWindow;

    private boolean printedMissingWarning;

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
        if (windowConfig == null) {
            windowConfig = new LoopStartWindowConfiguration();
            setWarningMessage("Using default: " + windowConfig);
        }

        DataTableSpec tableSpec = inSpecs[0];

        if (windowConfig.getTrigger() == Trigger.TIME && !tableSpec.containsName(timeColumnName)) {
            throw new InvalidSettingsException(
                "Selected time column '" + timeColumnName + "' does not exist in input table.");
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
        rowCount = table.size();

        if (currRow == 0) {
            rowIterator = table.iterator();
            bufferedRows = new LinkedList<>();

            nColumns = table.getSpec().getNumColumns();
        }

        if (windowConfig.getTrigger().equals(Trigger.EVENT)) {
            /* If window is limited to the table all three variants are the same. */
            if (windowConfig.getLimitWindow()) {
                return executeForward(table, exec);
            }

            switch (windowConfig.getWindowDefinition()) {
                case BACKWARD:
                    return executeBackward(table, exec);

                case CENTRAL:
                    return executeCentral(table, exec);

                case FORWARD:
                    return executeForward(table, exec);

                default:
                    return null;
            }
        }

        int column = table.getDataTableSpec().findColumnIndex(timeColumnName);
        boolean duration =
            table.getDataTableSpec().getColumnSpec(column).getType().equals(DataType.getType(DurationCell.class));

        if (duration) {
            return executeDuration(table, exec);
        }

        return executeTemporal(table, exec);

    }

    /**
     * @param table
     * @param exec
     * @return
     */
    private BufferedDataTable[] executeTemporal(final BufferedDataTable table, final ExecutionContext exec) {
        int column = table.getDataTableSpec().findColumnIndex(timeColumnName);
        Duration startInterval = windowConfig.getStartDuration();
        Duration windowDuration = windowConfig.getWindowDuration();
        Temporal windowEnd = null;

        // To check if an overflow occurred concerning the current window
        boolean overflow = false;
        // To check if an overflow occurred concerning next starting temporal.
        lastWindow = false;

        /* Compute end duration of window and beginning of next duration*/
        if (nextStartTemporal == null && rowIterator.hasNext()) {
            DataRow first = rowIterator.next();

            /* Check if column only consists of missing values. */
            while (first.getCell(column).isMissing() && rowIterator.hasNext()) {
                first = rowIterator.next();

                if (!printedMissingWarning) {
                    printedMissingWarning = true;
                    getLogger().warn("Detected missing values for specified column; rows have been skipped.");
                }
            }

            if (first.getCell(column).isMissing()) {
                throw new IllegalArgumentException("Chosen column only contains missing values.");
            }

            Temporal firstStart = getTemporal(first.getCell(column));

            prevTemporal = firstStart;

            nextStartTemporal = firstStart.plus(startInterval);

            /* Check if the next starting temporal lies beyond the maximum temporal value. */
            if (compareTemporal(nextStartTemporal, firstStart) <= 0) {
                lastWindow = true;
            }

            /* Checks if window overflow occurs. */
            Temporal temp = firstStart.plus(windowDuration);
            if (compareTemporal(temp, firstStart) <= 0) {
                overflow = true;
            } else {
                windowEnd = temp;
            }

            bufferedRows.add(first);
        } else {
            Temporal nextTemporal = getTemporal(bufferedRows.getFirst().getCell(column));
            prevTemporal = nextTemporal;

            /* Checks if temporal overflow occurs. */
            Temporal temp = nextStartTemporal.plus(windowDuration);
            if (compareTemporal(temp, nextStartTemporal.minus(startInterval)) <= 0) {
                overflow = true;
            } else {
                windowEnd = temp;
                temp = nextStartTemporal.plus(startInterval);
                /* Check if the next starting temporal lies beyond the maximum temporal value. */
                if (compareTemporal(temp, nextStartTemporal) <= 0) {
                    lastWindow = true;
                } else {
                    nextStartTemporal = temp;
                }
            }
        }

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        Iterator<DataRow> bufferedIterator = bufferedRows.iterator();

        boolean allBufferedRowsInWindow = true;

        /* Add buffered rows. */
        while (bufferedIterator.hasNext()) {
            DataRow row = bufferedIterator.next();

            Temporal temp = getTemporal(row.getCell(column));

            /* Checks if all buffered rows are in the specified window. */
            if (!overflow && compareTemporal(temp, windowEnd) >= 0) {
                allBufferedRowsInWindow = false;
                break;
            }

            container.addRowToTable(row);
            currRow++;

            if (overflow || lastWindow || compareTemporal(getTemporal(row.getCell(column)), nextStartTemporal) < 0) {
                bufferedIterator.remove();
            }
        }

        /* Add newly read rows. */
        while (rowIterator.hasNext() && allBufferedRowsInWindow) {
            DataRow row = rowIterator.next();

            if (row.getCell(column).isMissing()) {
                if (!printedMissingWarning) {
                    printedMissingWarning = true;
                    getLogger().warn("Detected missing values for specified column; rows have been skipped.");
                }

                continue;
            }

            Temporal currTemporal = getTemporal(row.getCell(column));

            /* Check if table is sorted in non-descending order according to temporal column. */
            if (compareTemporal(currTemporal, prevTemporal) < 0) {
                throw new IllegalStateException("Table not in ascending order concerning chosen temporal column.");
            }

            prevTemporal = currTemporal;

            /* Add rows for next window into the buffer. */
            if (compareTemporal(currTemporal, nextStartTemporal) >= 0 && !overflow && !lastWindow) {
                bufferedRows.add(row);
            }

            /* Add row to current output. */
            if (overflow || compareTemporal(currTemporal, windowEnd) < 0) {
                container.addRowToTable(row);
                currRow++;
            } else {
                break;
            }
        }

        /* Find next entry that lies in a following window. */
        DataRow row = null;
        /* Close iterator if last window has been filled. */
        if (lastWindow) {
            rowIterator.close();
        } else if (!allBufferedRowsInWindow) {
            row = bufferedRows.remove();
        } else if (bufferedRows.size() == 0 && rowIterator.hasNext()) {
            row = rowIterator.next();

            while (row.getCell(column).isMissing() && rowIterator.hasNext()) {
                row = rowIterator.next();

                if (row.getCell(column).isMissing() && !printedMissingWarning) {
                    printedMissingWarning = true;
                    getLogger().warn("Detected missing values for specified column; rows have been skipped.");
                }
            }

            if (row.getCell(column).isMissing()) {
                row = null;

                if (!printedMissingWarning) {
                    printedMissingWarning = true;
                    getLogger().warn("Detected missing values for specified column; rows have been skipped.");
                }
            }
        } else if (!overflow && !bufferedRows.isEmpty()) {
            /* Checks if the next buffered row lies within the given window */
            if (compareTemporal(windowEnd.plus(startInterval), windowEnd) > 0) {
                Temporal temp = getTemporal(bufferedRows.getFirst().getCell(column));

                if (compareTemporal(temp, windowEnd.plus(startInterval)) >= 0) {
                    row = bufferedRows.removeFirst();
                }
            }
        }

        skipTemporalWindows(row, column, startInterval, windowDuration);

        container.close();

        return new BufferedDataTable[]{container.getTable()};
    }

    /**
     * @param row
     */
    private void skipTemporalWindows(DataRow row, final int column, final Duration startInterval,
        final Duration windowDuration) {
        while (row != null) {
            /* Check if current row lies beyond next starting temporal. */
            while (compareTemporal(getTemporal(row.getCell(column)), nextStartTemporal) < 0 && rowIterator.hasNext()) {
                DataRow temp = rowIterator.next();

                if (temp.getCell(column).isMissing()) {
                    if (!printedMissingWarning) {
                        printedMissingWarning = true;
                        getLogger().warn("Detected missing values for specified column; rows have been skipped.");
                    }

                    continue;
                }

                row = temp;
            }

            /* Checks if current row lies within next temporal window or if overflow of window occurs */
            if (compareTemporal(getTemporal(row.getCell(column)), nextStartTemporal.plus(windowDuration)) < 0
                || compareTemporal(nextStartTemporal.plus(windowDuration), nextStartTemporal) < 0) {
                bufferedRows.addFirst(row);
                break;
            } else if (compareTemporal(getTemporal(row.getCell(column)), nextStartTemporal) < 0
                && !rowIterator.hasNext()) {
                /* There are no more rows that could lie within an upcoming window. */
                break;
            }

            /* If next row lies beyond the defined next window move it until the rows lies within an upcoming window or the window passed said row. */
            Temporal nextTemporalStart = nextStartTemporal.plus(startInterval);

            /* Check for overflow of the next starting interval. */
            if (compareTemporal(nextTemporalStart, nextStartTemporal) <= 0) {
                rowIterator.close();
                break;
            } else {
                nextStartTemporal = nextTemporalStart;
                /* Get next row s.t. temporal of row is greater than next starting temporal. */
                //                while (compareTemporal(getTemporal(row.getCell(column)), nextTemporalStart) < 0
                //                 hos   && m_iterator.hasNext()) {
                //                    row = m_iterator.next();
                //                }
                //
                //                /* Check if the current found temporal of row is greater than next starting temporal. */
                //                if (compareTemporal(getTemporal(row.getCell(column)), nextTemporalStart) < 0) {
                //                    break;
                //                }
                //
                //                /* Check if current point lies within the given window. */
                //                Temporal nextEndTemporal = nextTemporalStart.plus(windowDuration);
                //                if (compareTemporal(getTemporal(row.getCell(column)), nextEndTemporal) < 0
                //                    || compareTemporal(nextEndTemporal, nextTemporalStart) < 0) {
                //                    bufferedRows.add(row);
                //                    break;
                //                }
            }
        }

    }

    /**
     * @param nextRow
     * @param endTemporal
     * @return
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

        throw new IllegalArgumentException("Data must be of type LocalDate, LocalDateTime, or LocalTime");
    }

    /**
     * @param table
     * @param exec
     * @return
     */
    private BufferedDataTable[] executeDuration(final BufferedDataTable table, final ExecutionContext exec) {
        int column = table.getDataTableSpec().findColumnIndex(timeColumnName);
        Duration startInterval = windowConfig.getStartDuration();
        Duration windowDuration = windowConfig.getWindowDuration();
        Duration windowEnd = null;

        // To check if an overflow occurred concerning the current window
        boolean overflow = false;
        // To check if an overflow occurred concerning next starting temporal.
        lastWindow = false;

        /* Compute end duration of window and beginning of next duration*/
        if (nextStartDuration == null && rowIterator.hasNext()) {
            DataRow first = rowIterator.next();

            /* Check if column only consists of missing values. */
            while (first.getCell(column).isMissing() && rowIterator.hasNext()) {
                first = rowIterator.next();

                if (!printedMissingWarning) {
                    printedMissingWarning = true;
                    getLogger().warn("Detected missing values for specified column; rows have been skipped.");
                }
            }

            if (first.getCell(column).isMissing()) {
                throw new IllegalArgumentException("Chosen column only contains missing values.");
            }

            Duration firstStart = ((DurationCell)first.getCell(column)).getDuration();

            prevDuration = firstStart;

            nextStartDuration = firstStart.plus(startInterval);

            /* Check if the next starting temporal lies beyond the maximum temporal value. */
            if (nextStartDuration.compareTo(firstStart) <= 0) {
                lastWindow = true;
            }

            /* Checks if window overflow occurs. */
            Duration temp = firstStart.plus(windowDuration);
            if (temp.compareTo(firstStart) <= 0) {
                overflow = true;
            } else {
                windowEnd = temp;
            }

            bufferedRows.add(first);
        } else {
            Duration nextDuration = ((DurationCell)bufferedRows.getFirst().getCell(column)).getDuration();
            prevDuration = nextDuration;

            /* Checks if temporal overflow occurs. */
            Duration temp = nextStartDuration.plus(windowDuration);
            if (temp.compareTo(nextStartDuration.minus(startInterval)) <= 0) {
                overflow = true;
            } else {
                windowEnd = temp;
                temp = nextStartDuration.plus(startInterval);
                /* Check if the next starting temporal lies beyond the maximum temporal value. */
                if (temp.compareTo(nextStartDuration) <= 0) {
                    lastWindow = true;
                } else {
                    nextStartDuration = temp;
                }
            }
        }

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        Iterator<DataRow> bufferedIterator = bufferedRows.iterator();

        boolean allBufferedRowsInWindow = true;

        /* Add buffered rows. */
        while (bufferedIterator.hasNext()) {
            DataRow row = bufferedIterator.next();

            Duration temp = ((DurationCell)row.getCell(column)).getDuration();

            /* Checks if all buffered rows are in the specified window. */
            if (!overflow && temp.compareTo(windowEnd) >= 0) {
                allBufferedRowsInWindow = false;
                break;
            }

            container.addRowToTable(row);
            currRow++;

            if (overflow || lastWindow
                || ((DurationCell)row.getCell(column)).getDuration().compareTo(nextStartDuration) < 0) {
                bufferedIterator.remove();
            }
        }

        /* Add newly read rows. */
        while (rowIterator.hasNext() && allBufferedRowsInWindow) {
            DataRow row = rowIterator.next();

            if (row.getCell(column).isMissing()) {
                if (!printedMissingWarning) {
                    printedMissingWarning = true;
                    getLogger().warn("Detected missing values for specified column; rows have been skipped.");
                }

                continue;
            }

            Duration currDuration = ((DurationCell)row.getCell(column)).getDuration();

            /* Check if table is sorted in non-descending order according to temporal column. */
            if (currDuration.compareTo(prevDuration) < 0) {
                throw new IllegalStateException("Table not in ascending order concerning chosen temporal column.");
            }

            prevDuration = currDuration;

            /* Add rows for next window into the buffer. */
            if (currDuration.compareTo(nextStartDuration) >= 0 && !overflow && !lastWindow) {
                bufferedRows.add(row);
            }

            /* Add row to current output. */
            if (overflow || currDuration.compareTo(windowEnd) < 0) {
                container.addRowToTable(row);
                currRow++;
            } else {
                break;
            }
        }

        /* Find next entry that lies in a following window. */
        DataRow row = null;
        /* Close iterator if last window has been filled. */
        if (lastWindow) {
            rowIterator.close();
        } else if (!allBufferedRowsInWindow) {
            row = bufferedRows.remove();
        } else if (bufferedRows.size() == 0 && rowIterator.hasNext()) {
            row = rowIterator.next();

            while (row.getCell(column).isMissing() && rowIterator.hasNext()) {
                row = rowIterator.next();

                if (row.getCell(column).isMissing() && !printedMissingWarning) {
                    printedMissingWarning = true;
                    getLogger().warn("Detected missing values for specified column; rows have been skipped.");
                }
            }

            if (row.getCell(column).isMissing()) {
                row = null;

                if (!printedMissingWarning) {
                    printedMissingWarning = true;
                    getLogger().warn("Detected missing values for specified column; rows have been skipped.");
                }
            }
        } else if (!overflow && !bufferedRows.isEmpty()) {
            /* Checks if the next buffered row lies within the given window */
            if (windowEnd.plus(startInterval).compareTo(windowEnd) > 0) {
                Duration temp = ((DurationCell)bufferedRows.getFirst().getCell(column)).getDuration();

                if (temp.compareTo(windowEnd.plus(startInterval)) >= 0) {
                    row = bufferedRows.removeFirst();
                }
            }
        }

        skipDurationWindows(row, column, startInterval, windowDuration);

        container.close();

        return new BufferedDataTable[]{container.getTable()};
        //        int column = table.getDataTableSpec().findColumnIndex(timeColumnName);
        //
        //        Duration startInterval = windowConfig.getStartDuration();
        //        Duration windowDuration = windowConfig.getWindowDuration();
        //        Duration endDuration = null;
        //
        //        /* Compute end duration of window and beginning of next duration*/
        //        if (nextStartDuration == null && m_iterator.hasNext()) {
        //            DataRow first = m_iterator.next();
        //
        //            Duration firstStart = ((DurationCell)first.getCell(column)).getDuration();
        //            prevDuration = firstStart;
        //
        //            nextStartDuration = firstStart.plus(startInterval);
        //            endDuration = firstStart.plus(windowDuration);
        //
        //            bufferedRows.add(first);
        //        } else {
        //            /* Move window by interval until it contains at least one row. */
        //            if (bufferedRows.isEmpty() && m_iterator.hasNext()) {
        //                bufferedRows.add(m_iterator.next());
        //            }
        //
        //            Duration nextRow = ((DurationCell)bufferedRows.getFirst().getCell(column)).getDuration();
        //
        //            prevDuration = nextRow;
        //
        //            do {
        //                endDuration = nextStartDuration.plus(windowDuration);
        //                nextStartDuration = nextStartDuration.plus(startInterval);
        //            } while (nextRow.compareTo(endDuration) >= 0);
        //        }
        //
        //        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        //        Iterator<DataRow> bufferedIterator = bufferedRows.iterator();
        //
        //        /* Add buffered rows. */
        //        while (bufferedIterator.hasNext()) {
        //            DataRow row = bufferedIterator.next();
        //            container.addRowToTable(row);
        //            currRow++;
        //
        //            if (((DurationCell)row.getCell(column)).getDuration().compareTo(nextStartDuration) < 0) {
        //                bufferedIterator.remove();
        //            }
        //        }
        //
        //        /* Add newly read rows. */
        //        while (m_iterator.hasNext()) {
        //            DataRow row = m_iterator.next();
        //
        //            Duration currDuration = ((DurationCell)row.getCell(column)).getDuration();
        //
        //            /* Check if table is sorted according to temporal column. */
        //            if (currDuration.compareTo(prevDuration) < 0) {
        //                throw new IllegalStateException("Table not in ascending order concerning chosen temporal column.");
        //            }
        //
        //            prevDuration = currDuration;
        //
        //            /* Add overlapping rows to the buffer. */
        //            if (currDuration.compareTo(nextStartDuration) >= 0) {
        //                bufferedRows.add(row);
        //                currRow++;
        //            }
        //
        //            /* Add row to current output. */
        //            if (currDuration.compareTo(endDuration) < 0) {
        //                container.addRowToTable(row);
        //            } else {
        //                break;
        //            }
        //        }
        //
        //        container.close();
        //
        //        return new BufferedDataTable[]{container.getTable()};
    }

    /**
     * @param row
     * @param column
     * @param startInterval
     * @param windowDuration
     */
    private void skipDurationWindows(DataRow row, final int column, final Duration startInterval,
        final Duration windowDuration) {
        while (row != null) {
            /* Check if current row lies beyond next starting temporal. */
            while (((DurationCell)row.getCell(column)).getDuration().compareTo(nextStartDuration) < 0
                && rowIterator.hasNext()) {
                DataRow temp = rowIterator.next();

                if (temp.getCell(column).isMissing()) {
                    if (!printedMissingWarning) {
                        printedMissingWarning = true;
                        getLogger().warn("Detected missing values for specified column; rows have been skipped.");
                    }

                    continue;
                }

                row = temp;
            }

            /* Checks if current row lies within next temporal window or if overflow of window occurs */
            if (((DurationCell)row.getCell(column)).getDuration().compareTo(nextStartDuration.plus(windowDuration)) < 0
                || nextStartDuration.plus(windowDuration).compareTo(nextStartDuration) < 0) {
                bufferedRows.addFirst(row);
                break;
            } else if (((DurationCell)row.getCell(column)).getDuration().compareTo(nextStartDuration) < 0
                && !rowIterator.hasNext()) {
                /* There are no more rows that could lie within an upcoming window. */
                break;
            }

            /* If next row lies beyond the defined next window move it until the rows lies within an upcoming window or the window passed said row. */
            Duration nextDurationStart = nextStartDuration.plus(startInterval);

            /* Check for overflow of the next starting interval. */
            if (nextDurationStart.compareTo(nextStartDuration) <= 0) {
                rowIterator.close();
                break;
            } else {
                nextStartDuration = nextDurationStart;
            }
        }
    }

    /**
     * @param cell
     * @return
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
        int windowSize = windowConfig.getWindowSize();
        int stepSize = windowConfig.getStepSize();
        int currRowCount = 0;

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        Iterator<DataRow> bufferedIterator = bufferedRows.iterator();

        /* Jump to next following row if step size is greater than the window size. */
        if (stepSize > windowSize && currRow > 0) {
            int diff = stepSize - windowSize;

            while (diff > 0 && rowIterator.hasNext()) {
                rowIterator.next();
                diff--;
            }
        }

        /* Add missing preceding rows to fill up the window at the beginning of the loop. */
        while (container.size() < windowSize - (currRow + 1)) {
            container.addRowToTable(new MissingRow(nColumns));
            currRowCount++;
        }

        /* Add buffered rows that overlap. */
        while (bufferedIterator.hasNext()) {
            container.addRowToTable(bufferedIterator.next());

            if (currRowCount < stepSize) {
                bufferedIterator.remove();
            }

            currRowCount++;
        }

        /* Add newly read rows. */
        for (; container.size() < windowSize && rowIterator.hasNext(); currRowCount++) {
            DataRow dRow = rowIterator.next();

            if (currRowCount >= stepSize) {
                bufferedRows.add(dRow);
            }

            container.addRowToTable(dRow);
        }

        currRow += stepSize;

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
        int windowSize = windowConfig.getWindowSize();
        int stepSize = windowConfig.getStepSize();
        int currRowCount = 0;

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        Iterator<DataRow> bufferedIterator = bufferedRows.iterator();

        /* Fill missing preceding rows with missing values. Only needed at the start of*/
        while (container.size() < Math.floorDiv(windowSize, 2) - (currRow)) {
            container.addRowToTable(new MissingRow(nColumns));
            currRowCount++;
        }

        /* Jump to next following row if step size is greater than the window size.*/
        if (stepSize > windowSize && currRow > 0) {
            int diff = stepSize - windowSize;

            while (diff > 0 && rowIterator.hasNext()) {
                rowIterator.next();
                diff--;
            }
        }

        /* Add buffered rows that overlap. */
        while (bufferedIterator.hasNext()) {
            container.addRowToTable(bufferedIterator.next());

            if (currRowCount < stepSize) {
                bufferedIterator.remove();
            }

            currRowCount++;
        }

        /* Add newly read rows. */
        for (; container.size() < windowSize && rowIterator.hasNext(); currRowCount++) {
            DataRow dRow = rowIterator.next();

            if (currRowCount >= stepSize) {
                bufferedRows.add(dRow);
            }

            container.addRowToTable(dRow);
        }

        /* Add missing rows to fill up the window. */
        while (container.size() < windowSize) {
            container.addRowToTable(new MissingRow(nColumns));
        }

        currRow += stepSize;

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
        int windowSize = windowConfig.getWindowSize();
        int stepSize = windowConfig.getStepSize();
        int currRowCount = 0;

        /* Jump to next following row if step size is greater than the window size.*/
        if (stepSize > windowSize && currRow > 0) {
            int diff = stepSize - windowSize;

            while (diff > 0 && rowIterator.hasNext()) {
                rowIterator.next();
                diff--;
            }
        }

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        Iterator<DataRow> bufferedIterator = bufferedRows.iterator();

        /* Add buffered rows that overlap. */
        while (bufferedIterator.hasNext()) {
            container.addRowToTable(bufferedIterator.next());

            if (currRowCount < stepSize) {
                bufferedIterator.remove();
            }

            currRowCount++;
        }

        /* Add newly read rows. */
        for (; container.size() < windowSize && rowIterator.hasNext(); currRowCount++) {
            DataRow dRow = rowIterator.next();

            if (currRowCount >= stepSize) {
                bufferedRows.add(dRow);
            }

            container.addRowToTable(dRow);
        }

        /* Add missing rows to fill up the window. */
        while (container.size() < windowSize) {
            container.addRowToTable(new MissingRow(nColumns));
        }

        currRow += stepSize;

        container.close();

        return new BufferedDataTable[]{container.getTable()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        currRow = 0;
        MissingRow.rowCounter = 0;
        lastWindow = false;
        printedMissingWarning = false;
        //        m_iteration = 0;
        if (rowIterator != null) {
            rowIterator.close();
        }
        rowIterator = null;
        //        m_table = null;
        nextStartDuration = null;
        nextStartTemporal = null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean terminateLoop() {
        if (windowConfig.getTrigger().equals(Trigger.EVENT)) {
            /* If we limit the window to fit in the table we might terminate earlier. */
            if (windowConfig.getLimitWindow()) {
                return rowCount - currRow < windowConfig.getWindowSize();
            }

            return currRow >= rowCount;
        }

        return lastWindow || !rowIterator.hasNext() && (bufferedRows == null || bufferedRows.isEmpty());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (windowConfig != null) {
            windowConfig.saveSettingsTo(settings);
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
        windowConfig = config;

        SettingsModelString settingsModel = LoopStartWindowNodeDialogPane.createColumnModel();
        settingsModel.loadSettingsFrom(settings);
        timeColumnName = settingsModel.getStringValue();
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
