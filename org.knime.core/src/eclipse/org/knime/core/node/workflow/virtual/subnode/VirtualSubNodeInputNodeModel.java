/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by
 *  University of Konstanz, Germany and
 *  KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *   Apr 7, 2014 (wiswedel): created
 */
package org.knime.core.node.workflow.virtual.subnode;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;
import org.knime.core.node.util.filter.variable.FlowVariableFilterConfiguration;
import org.knime.core.node.workflow.FlowObjectStack;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.SubNodeContainer;

/**
 * NodeModel of the subnode virtual end node.
 * <p>No API.
 * @author Bernd Wiswedel, KNIME.com, Zurich, Switzerland
 */
public final class VirtualSubNodeInputNodeModel extends NodeModel {

    /** Needed to fetch data and flow object stack. */
    private int m_numberOfPorts;
    private SubNodeContainer m_subNodeContainer;
    private VirtualSubNodeInputConfiguration m_configuration;

    /**
     * @param subnodeContainer
     * @param outPortTypes
     */
    VirtualSubNodeInputNodeModel(final SubNodeContainer subnodeContainer, final PortType[] outPortTypes) {
        super(new PortType[0], outPortTypes);
        m_numberOfPorts = outPortTypes.length;
        m_subNodeContainer = subnodeContainer;
        m_configuration = VirtualSubNodeInputConfiguration.newDefault(m_numberOfPorts);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void onDispose() {
        super.onDispose();
        m_subNodeContainer = null;
    }

    public FlowObjectStack getSubNodeContainerFlowObjectStack() {
        return m_subNodeContainer.getFlowObjectStack();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec)
            throws Exception {
        CheckUtils.checkNotNull(m_subNodeContainer, "No subnode container set");
        PortObject[] dataFromParent = m_subNodeContainer.fetchInputDataFromParent();
        if (dataFromParent == null) {
            setWarningMessage("Not all inputs available");
            Thread.currentThread().interrupt();
            return null;
        }
        String prefix = m_configuration.getFlowVariablePrefix() == null ? "" : m_configuration.getFlowVariablePrefix();
        FlowVariableFilterConfiguration filterConfiguration = m_configuration.getFilterConfiguration();
        Map<String, FlowVariable> availableVariables = getSubNodeContainerFlowObjectStack().getAvailableFlowVariables();
        FilterResult filtered = filterConfiguration.applyTo(availableVariables);
        for (String include : filtered.getIncludes()) {
            FlowVariable f = availableVariables.get(include);
            switch (f.getScope()) {
                case Global:
                    // ignore global flow vars
                    continue;
                case Flow:
                case Local:
                default:
            }
            switch (f.getType()) {
            case DOUBLE:
                pushFlowVariableDouble(prefix + f.getName(), f.getDoubleValue());
                break;
            case INTEGER:
                pushFlowVariableInt(prefix + f.getName(), f.getIntValue());
                break;
            case STRING:
                pushFlowVariableString(prefix + f.getName(), f.getStringValue());
                break;
            default:
                throw new Exception("Unsupported flow variable type: " + f.getType());
            }
        }
        return ArrayUtils.removeAll(dataFromParent, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs)
            throws InvalidSettingsException {
        CheckUtils.checkNotNull(m_subNodeContainer, "No subnode container set");
        if (m_configuration == null) {
            setWarningMessage("Guessing defaults (excluding all variables)");
            m_configuration = VirtualSubNodeInputConfiguration.newDefault(m_numberOfPorts);
        }
        PortObjectSpec[] specsFromParent = m_subNodeContainer.fetchInputSpecFromParent();
        return ArrayUtils.removeAll(specsFromParent, 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_configuration != null) {
            m_configuration.saveConfiguration(settings);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        new VirtualSubNodeInputConfiguration(m_numberOfPorts).loadConfigurationInModel(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        VirtualSubNodeInputConfiguration config = new VirtualSubNodeInputConfiguration(m_numberOfPorts);
        config.loadConfigurationInModel(settings);
        m_configuration = config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // no internals
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // no internals
    }

    /**
     * @param subNodeContainer
     */
    public void setSubNodeContainer(final SubNodeContainer subNodeContainer) {
        m_subNodeContainer = subNodeContainer;
    }

    /**
     * @return Description for the sub node
     */
    public String getSubNodeDescription() {
        return m_configuration.getSubNodeDescription();
    }

    /**
     * @return Names of the ports
     */
    public String[] getPortNames() {
        return m_configuration.getPortNames();
    }

    /**
     * @return Descriptions of the ports
     */
    public String[] getPortDescriptions() {
        return m_configuration.getPortDescriptions();
    }

}