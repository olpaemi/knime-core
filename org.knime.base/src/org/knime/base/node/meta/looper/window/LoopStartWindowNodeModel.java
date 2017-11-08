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
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.workflow.LoopStartNodeTerminator;

/**
 * Loop start node that outputs a set of rows at a time. Used to implement a streaming (or chunking approach) where only
 * a set of rows is processed at a time
 *
 * @author Bernd Wiswedel, KNIME AG, Zurich, Switzerland
 */
public class LoopStartWindowNodeModel extends NodeModel implements LoopStartNodeTerminator {

    private LoopStartWindowConfiguration m_config;

    // loop invariants
    // private BufferedDataTable m_table;

    private CloseableRowIterator m_iterator;

    // loop variants
    // private int m_iteration;

    // number of columns
    private int nColumns;

    // index of current row
    private long currRow;

    private long rowCount;

    // buffered rows used for overlapping
    private LinkedList<DataRow> bufferedRows;

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
        if (m_config == null) {
            m_config = new LoopStartWindowConfiguration();
            setWarningMessage("Using default: " + m_config);
        }
        //        assert m_iteration == 0;
        //        pushFlowVariableInt("currentIteration", m_iteration);
        //        pushFlowVariableInt("maxIterations", 0);
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
            m_iterator = table.iterator();
            bufferedRows = new LinkedList<>();

            nColumns = table.getSpec().getNumColumns();
        }

        if (m_config.getTrigger().equals(Trigger.EVENT)) {
            switch (m_config.getWindowDefinition()) {
                case BACKWARD:
                    return executeBackward(table, exec);

                case CENTRAL:
                    return executeCentral(table, exec);

                case FORWARD:
                    return executeForward(table, exec);

                default:
                    return null;
            }
        } else {
            return null;
        }
    }

    /**
     * Executes backward windowing.
     *
     * @param table input data
     * @param exec ExecutionContext
     * @return BufferedDataTable containing the current loop.
     */
    private BufferedDataTable[] executeBackward(final BufferedDataTable table, final ExecutionContext exec) {
        int windowSize = m_config.getWindowSize();
        int stepSize = m_config.getStepSize();
        int currRowCount = 0;

        BufferedDataContainer container = exec.createDataContainer(table.getSpec());
        Iterator<DataRow> bufferedIterator = bufferedRows.iterator();

        /* Jump to next following row if step size is greater than the window size. */
        if (stepSize > windowSize && currRow > 0) {
            int diff = stepSize - windowSize;

            while (diff > 0 && m_iterator.hasNext()) {
                m_iterator.next();
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
        for (; container.size() < windowSize && m_iterator.hasNext(); currRowCount++) {
            DataRow dRow = m_iterator.next();

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
        int windowSize = m_config.getWindowSize();
        int stepSize = m_config.getStepSize();
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

            while (diff > 0 && m_iterator.hasNext()) {
                m_iterator.next();
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
        for (; container.size() < windowSize && m_iterator.hasNext(); currRowCount++) {
            DataRow dRow = m_iterator.next();

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
        int windowSize = m_config.getWindowSize();
        int stepSize = m_config.getStepSize();
        int currRowCount = 0;

        /* Jump to next following row if step size is greater than the window size.*/
        if (stepSize > windowSize && currRow > 0) {
            int diff = stepSize - windowSize;

            while (diff > 0 && m_iterator.hasNext()) {
                m_iterator.next();
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
        for (; container.size() < windowSize && m_iterator.hasNext(); currRowCount++) {
            DataRow dRow = m_iterator.next();

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
        //        m_iteration = 0;
        if (m_iterator != null) {
            m_iterator.close();
        }
        m_iterator = null;
        //        m_table = null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean terminateLoop() {
        return currRow >= rowCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_config != null) {
            m_config.saveSettingsTo(settings);
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
        m_config = config;
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
