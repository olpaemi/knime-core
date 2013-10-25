/*
 * ------------------------------------------------------------------------
 *
 *  Copyright (C) 2003 - 2013
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
 * Created on Sep 30, 2013 by Berthold
 */
package org.knime.core.node.workflow;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.knime.core.data.container.ContainerTable;
import org.knime.core.data.filestore.internal.FileStoreHandlerRepository;
import org.knime.core.data.filestore.internal.IFileStoreHandler;
import org.knime.core.data.filestore.internal.ILoopStartWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.IWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.LoopEndWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.LoopStartReferenceWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.LoopStartWritableFileStoreHandler;
import org.knime.core.data.filestore.internal.ReferenceWriteFileStoreHandler;
import org.knime.core.data.filestore.internal.WorkflowFileStoreHandlerRepository;
import org.knime.core.data.filestore.internal.WriteFileStoreHandler;
import org.knime.core.internal.ReferencedFile;
import org.knime.core.node.AbstractNodeView;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.DataAwareNodeDialogPane;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.Node;
import org.knime.core.node.NodeConfigureHelper;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeFactory.NodeType;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeProgressMonitor;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.interactive.InteractiveView;
import org.knime.core.node.interactive.ViewContent;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.property.hilite.HiLiteHandler;
import org.knime.core.node.web.WebTemplate;
import org.knime.core.node.workflow.FlowVariable.Scope;
import org.knime.core.node.workflow.WorkflowPersistor.LoadResult;
import org.knime.core.node.workflow.execresult.NodeContainerExecutionResult;
import org.knime.core.node.workflow.execresult.NodeContainerExecutionStatus;
import org.knime.core.node.workflow.execresult.NodeExecutionResult;
import org.knime.core.node.workflow.execresult.SingleNodeContainerExecutionResult;
import org.w3c.dom.Element;

/**
 * Implementation of {@link SingleNodeContainer} for a natively implemented KNIME Node relying
 * on a {@link NodeModel}.
 *
 * @author B. Wiswedel & M.Berthold
 * @since 2.9
 */
public class NativeNodeContainer extends SingleNodeContainer {

    /** my logger. */
    private static final NodeLogger LOGGER =
        NodeLogger.getLogger(NativeNodeContainer.class);


    /** underlying node. */
    private final Node m_node;

    /**
     * Create new SingleNodeContainer based on existing Node.
     *
     * @param parent the workflow manager holding this node
     * @param n the underlying node
     * @param id the unique identifier
     */
    NativeNodeContainer(final WorkflowManager parent, final Node n, final NodeID id) {
        super(parent, id);
        m_node = n;
        setPortNames();
        m_node.addMessageListener(new UnderlyingNodeMessageListener());
    }

    /**
     * Create new SingleNodeContainer from persistor.
     *
     * @param parent the workflow manager holding this node
     * @param id the identifier
     * @param persistor to read from
     */
    NativeNodeContainer(final WorkflowManager parent, final NodeID id, final NativeNodeContainerPersistor persistor) {
        super(parent, id, persistor.getMetaPersistor());
        m_node = persistor.getNode();
        assert m_node != null : persistor.getClass().getSimpleName()
                + " did not provide Node instance for "
                + getClass().getSimpleName() + " with id \"" + id + "\"";
        setPortNames();
        m_node.addMessageListener(new UnderlyingNodeMessageListener());
    }

    /** The message listener that is added the Node and listens for messages
     * that are set by failing execute methods are by the user
     * (setWarningMessage()).
     */
    private final class UnderlyingNodeMessageListener
        implements NodeMessageListener {
        /** {@inheritDoc} */
        @Override
        public void messageChanged(final NodeMessageEvent messageEvent) {
            NativeNodeContainer.this.setNodeMessage(messageEvent.getMessage());
        }
    }

    /** Get the underlying node.
     * @return the underlying Node
     */
    public Node getNode() {
        return m_node;
    }

    /**
     * @return reference to underlying node.
     * @deprecated Method is going to be removed in future versions. Use
     * {@link #getNode()} instead.
     * Currently used to enable workaround for bug #2136 (see also bug #2137)
     */
    @Deprecated
    public Node getNodeReferenceBug2136() {
        return getNode();
    }

    /** @return reference to underlying node's model. */
    public NodeModel getNodeModel() {
        return getNode().getNodeModel();
    }

    /* ------------------ Port Handling ------------- */

    /* */
    private void setPortNames() {
        for (int i = 0; i < getNrOutPorts(); i++) {
            getOutPort(i).setPortName(m_node.getOutportDescriptionName(i));
        }
        for (int i = 0; i < getNrInPorts(); i++) {
            getInPort(i).setPortName(m_node.getInportDescriptionName(i));
        }
    }

    /** {@inheritDoc} */
    @Override
    public int getNrOutPorts() {
        return m_node.getNrOutPorts();
    }

    /** {@inheritDoc} */
    @Override
    public int getNrInPorts() {
        return m_node.getNrInPorts();
    }

    private NodeContainerOutPort[] m_outputPorts = null;
    /**
     * Returns the output port for the given <code>portID</code>. This port
     * is essentially a container for the underlying Node and the index and will
     * retrieve all interesting data from the Node.
     *
     * @param index The output port's ID.
     * @return Output port with the specified ID.
     * @throws IndexOutOfBoundsException If the index is out of range.
     */
    @Override
    public NodeOutPort getOutPort(final int index) {
        if (m_outputPorts == null) {
            m_outputPorts = new NodeContainerOutPort[getNrOutPorts()];
        }
        if (m_outputPorts[index] == null) {
            m_outputPorts[index] = new NodeContainerOutPort(this, index);
        }
        return m_outputPorts[index];
    }

    private NodeInPort[] m_inputPorts = null;
    /**
     * Return a port, which for the inputs really only holds the type and some
     * other static information.
     *
     * @param index the index of the input port
     * @return port
     */
    @Override
    public NodeInPort getInPort(final int index) {
        if (m_inputPorts == null) {
            m_inputPorts = new NodeInPort[getNrInPorts()];
        }
        if (m_inputPorts[index] == null) {
            m_inputPorts[index] = new NodeInPort(index, m_node.getInputType(index));
        }
        return m_inputPorts[index];
    }

    /* ------------------ Views ---------------- */

    /**
     * {@inheritDoc}
     */
    @Override
    void setInHiLiteHandler(final int index, final HiLiteHandler hdl) {
        m_node.setInHiLiteHandler(index, hdl);
    }

    /** {@inheritDoc} */
    @Override
    public AbstractNodeView<NodeModel> getNodeView(final int i) {
        String title = getNameWithID() + " (" + getViewName(i) + ")";
        String customName = getDisplayCustomLine();
        if (!customName.isEmpty()) {
            title += " - " + customName;
        }
        NodeContext.pushContext(this);
        try {
            return (AbstractNodeView<NodeModel>)m_node.getView(i, title);
        } finally {
            NodeContext.removeLastContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public String getNodeViewName(final int i) {
        return m_node.getViewName(i);
    }

    /** {@inheritDoc} */
    @Override
    public int getNrNodeViews() {
        return m_node.getNrViews();
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasInteractiveView() {
        return m_node.hasInteractiveView();
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasInteractiveWebView() {
        return m_node.hasWizardView();
    }

    /** {@inheritDoc} */
    @Override
    public String getInteractiveViewName() {
        return m_node.getInteractiveViewName();
    }

    /** {@inheritDoc} */
    @Override
    public <V extends AbstractNodeView<?> & InteractiveView<?, ? extends ViewContent>> V getInteractiveView() {
        NodeContext.pushContext(this);
        try {
            V ainv = m_node.getNodeModel().getInteractiveNodeView();
            if (ainv == null) {
                String name = getInteractiveViewName();
                if (name == null) {
                    name = "TITLE MISSING";
                }
                String title = getNameWithID() + " (" + name + ")";
                String customName = getDisplayCustomLine();
                if (!customName.isEmpty()) {
                    title += " - " + customName;
                }
                ainv = m_node.getInteractiveView(title);
                ainv.setWorkflowManagerAndNodeID(getParent(), getID());
            }
            return ainv;
        } finally {
            NodeContext.removeLastContext();
        }
    }

    
    /* ------------------ Job Handling ---------------- */

    /** {@inheritDoc} */
    @Override
    void cleanup() {
        super.cleanup();
        NodeContext.pushContext(this);
        try {
            clearFileStoreHandler();
            m_node.cleanup();
        } finally {
            NodeContext.removeLastContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    void setJobManager(final NodeExecutionJobManager je) {
        synchronized (m_nodeMutex) {
            switch (getInternalState()) {
            case CONFIGURED_QUEUED:
            case EXECUTED_QUEUED:
            case PREEXECUTE:
            case EXECUTING:
            case EXECUTINGREMOTELY:
            case POSTEXECUTE:
                throwIllegalStateException();
            default:
            }
            super.setJobManager(je);
        }
    }

    /** {@inheritDoc} */
    @Override
    public ExecutionContext createExecutionContext() {
        NodeProgressMonitor progressMonitor = getProgressMonitor();
        return new ExecutionContext(progressMonitor, getNode(),
                getOutDataMemoryPolicy(),
                getParent().getGlobalTableRepository());
    }

    /* ---------------- Configuration/Execution ----------------- */

    /** {@inheritDoc} */
    @Override
    boolean performConfigure(final PortObjectSpec[] inSpecs, final NodeConfigureHelper nch) {
        return m_node.configure(inSpecs, nch);
    }

    /** {@inheritDoc} */
    @Override
    void performLoadModelSettingsFrom(final NodeSettingsRO modelSettings) throws InvalidSettingsException {
        m_node.loadModelSettingsFrom(modelSettings);
    }

    /** {@inheritDoc} */
    @Override
    public NodeContainerExecutionStatus performExecuteNode(final PortObject[] inObjects) {
        IWriteFileStoreHandler fsh = initFileStore(getParent().getFileStoreHandlerRepository());
        m_node.setFileStoreHandler(fsh);
        // this call requires the FSH to be set on the node (ideally would take
        // it as an argument but createExecutionContext became API unfortunately)
        ExecutionContext ec = createExecutionContext();
        fsh.open(ec);
        ExecutionEnvironment ev = getExecutionEnvironment();

        boolean success;
        try {
            ec.checkCanceled();
            success = true;
        } catch (CanceledExecutionException e) {
            String errorString = "Execution canceled";
            LOGGER.warn(errorString);
            setNodeMessage(new NodeMessage(NodeMessage.Type.WARNING, errorString));
            success = false;
        }
        NodeContext.pushContext(this);
        try {
            // execute node outside any synchronization!
            success = success && m_node.execute(inObjects, ev, ec);
        } finally {
            NodeContext.removeLastContext();
        }
        if (success) {
            // output tables are made publicly available (for blobs)
            putOutputTablesIntoGlobalRepository(ec);
        } else {
            // something went wrong: reset and configure node to reach
            // a solid state again will be done by WorkflowManager (in
            // doAfterExecute().
        }
        return success ? NodeContainerExecutionStatus.SUCCESS : NodeContainerExecutionStatus.FAILURE;
    }


    /* ----------- Reset and Port handling ------------- */

    /** {@inheritDoc} */
    @Override
    void performReset() {
        m_node.reset();
        cleanOutPorts(false);
    }

    /** {@inheritDoc} */
    @Override
    void cleanOutPorts(final boolean isLoopRestart) {
        m_node.cleanOutPorts(isLoopRestart);
        if (!isLoopRestart) {
            // this should have no affect as m_node.cleanOutPorts() will remove
            // all tables already
            int nrRemovedTables = removeOutputTablesFromGlobalRepository();
            assert nrRemovedTables == 0 : nrRemovedTables + " tables in global "
                + "repository after node cleared outports (expected 0)";
        }
    }

    /**
     * Enumerates the output tables and puts them into the workflow global
     * repository of tables. All other (temporary) tables that were created in
     * the given execution context, will be put in a set of temporary tables in
     * the node.
     *
     * @param c The execution context containing the (so far) local tables.
     */
    private void putOutputTablesIntoGlobalRepository(final ExecutionContext c) {
        HashMap<Integer, ContainerTable> globalRep =
            getParent().getGlobalTableRepository();
        m_node.putOutputTablesIntoGlobalRepository(globalRep);
        HashMap<Integer, ContainerTable> localRep =
                Node.getLocalTableRepositoryFromContext(c);
        Set<ContainerTable> localTables = new HashSet<ContainerTable>();
        for (Map.Entry<Integer, ContainerTable> t : localRep.entrySet()) {
            ContainerTable fromGlob = globalRep.get(t.getKey());
            if (fromGlob == null) {
                // not used globally
                localTables.add(t.getValue());
            } else {
                assert fromGlob == t.getValue();
            }
        }
        m_node.addToTemporaryTables(localTables);
    }

    /** Removes all tables that were created by this node from the global
     * table repository. */
    private int removeOutputTablesFromGlobalRepository() {
        HashMap<Integer, ContainerTable> globalRep =
            getParent().getGlobalTableRepository();
        return m_node.removeOutputTablesFromGlobalRepository(globalRep);
    }

    /* --------------- File Store Handling ------------- */

    /** ...
     *
     * @param success ...
     */
    void closeFileStoreHandlerAfterExecute(final boolean success) {
        IFileStoreHandler fsh = m_node.getFileStoreHandler();
        if (fsh instanceof IWriteFileStoreHandler) {
            ((IWriteFileStoreHandler)fsh).close();
        } else {
            // can be null if run through 3rd party executor
            // might be not an IWriteFileStoreHandler if restored loop is executed
            // (this will result in a failure before model#execute is called)
            assert !success || fsh == null
            : "must not be " + fsh.getClass().getSimpleName() + " in execute";
            LOGGER.debug("Can't close file store handler, not writable: "
                    + (fsh == null ? "<null>" : fsh.getClass().getSimpleName()));
        }
    }

    private IWriteFileStoreHandler initFileStore(
        final WorkflowFileStoreHandlerRepository fileStoreHandlerRepository) {
        FlowLoopContext upstreamFLC = getFlowObjectStack().peek(FlowLoopContext.class);
        NodeID outerStartNodeID = upstreamFLC == null ? null : upstreamFLC.getHeadNode();
        // loop start nodes will put their loop context on the outgoing flow object stack
        assert !getID().equals(outerStartNodeID) : "Loop start on incoming flow stack can't be node itself";

        FlowLoopContext innerFLC = getOutgoingFlowObjectStack().peek(FlowLoopContext.class);
        NodeID innerStartNodeID = innerFLC == null ? null : innerFLC.getHeadNode();
        // if there is a loop context on this node's stack, this node must be the start
        assert !(this.isModelCompatibleTo(LoopStartNode.class)) || getID().equals(innerStartNodeID);

        IFileStoreHandler oldFSHandler = m_node.getFileStoreHandler();
        IWriteFileStoreHandler newFSHandler;

        if (innerFLC == null && upstreamFLC == null) {
            // node is not a start node and not contained in a loop
            if (oldFSHandler instanceof IWriteFileStoreHandler) {
                clearFileStoreHandler();
                /*assert false : "Node " + getNameWithID() + " must not have file store handler at this point (not a "
                + "loop start and not contained in loop), disposing old handler";*/
            }
            newFSHandler = new WriteFileStoreHandler(getNameWithID(), UUID.randomUUID());
            newFSHandler.addToRepository(fileStoreHandlerRepository);
        } else if (innerFLC != null) {
            // node is a loop start node
            int loopIteration = innerFLC.getIterationIndex();
            if (loopIteration == 0) {
                if (oldFSHandler instanceof IWriteFileStoreHandler) {
                    assert false : "Loop Start " + getNameWithID() + " must not have file store handler at this point "
                            + "(no iteration ran), disposing old handler";
                clearFileStoreHandler();
                }
                if (upstreamFLC != null) {
                    ILoopStartWriteFileStoreHandler upStreamFSHandler = upstreamFLC.getFileStoreHandler();
                    newFSHandler = new LoopStartReferenceWriteFileStoreHandler(upStreamFSHandler, innerFLC);
                } else {
                    newFSHandler = new LoopStartWritableFileStoreHandler(getNameWithID(), UUID.randomUUID(), innerFLC);
                }
                newFSHandler.addToRepository(fileStoreHandlerRepository);
                innerFLC.setFileStoreHandler((ILoopStartWriteFileStoreHandler)newFSHandler);
            } else {
                assert oldFSHandler instanceof IWriteFileStoreHandler : "Loop Start " + getNameWithID()
                + " must have file store handler in iteration " + loopIteration;
            newFSHandler = (IWriteFileStoreHandler)oldFSHandler;
            // keep the old one
            }
        } else {
            // ordinary node contained in loop
            assert innerFLC == null && upstreamFLC != null;
            ILoopStartWriteFileStoreHandler upStreamFSHandler = upstreamFLC.getFileStoreHandler();
            if (this.isModelCompatibleTo(LoopEndNode.class)) {
                if (upstreamFLC.getIterationIndex() > 0) {
                    newFSHandler = (IWriteFileStoreHandler)oldFSHandler;
                } else {
                    newFSHandler = new LoopEndWriteFileStoreHandler(upStreamFSHandler);
                    newFSHandler.addToRepository(fileStoreHandlerRepository);
                }
            } else {
                newFSHandler = new ReferenceWriteFileStoreHandler(upStreamFSHandler);
                newFSHandler.addToRepository(fileStoreHandlerRepository);
            }
        }
        return newFSHandler;
    }

    /** Disposes file store handler (if set) and sets it to null. Called from reset and cleanup.
     * @noreference This method is not intended to be referenced by clients. */
    public void clearFileStoreHandler() {
        IFileStoreHandler fileStoreHandler = m_node.getFileStoreHandler();
        if (fileStoreHandler != null) {
            fileStoreHandler.clearAndDispose();
            m_node.setFileStoreHandler(null);
        }
    }

    /* --------------- Loop Stuff ----------------- */

    /** Possible loop states. */
    public static enum LoopStatus { NONE, RUNNING, PAUSED, FINISHED }
    /**
     * @return status of loop (determined from NodeState and LoopContext)
     */
    public LoopStatus getLoopStatus() {
        if (this.isModelCompatibleTo(LoopEndNode.class)) {
            if ((getNode().getLoopContext() != null)
                    || (getInternalState().isExecutionInProgress())) {
                if ((getNode().getPauseLoopExecution())
                        && (getInternalState().equals(InternalNodeContainerState.CONFIGURED_MARKEDFOREXEC))) {
                    return LoopStatus.PAUSED;
                } else {
                    return LoopStatus.RUNNING;
                }
            } else {
                return LoopStatus.FINISHED;
            }
        }
        return LoopStatus.NONE;
    }

    /**
     * @see NodeModel#resetAndConfigureLoopBody()
     */
    boolean resetAndConfigureLoopBody() {
        NodeContext.pushContext(this);
        try {
            return getNode().resetAndConfigureLoopBody();
        } finally {
            NodeContext.removeLastContext();
        }
    }

    /** enable (or disable) that after the next execution of this loop end node
     * the execution will be halted. This can also be called on a paused node
     * to trigger a "single step" execution.
     *
     * @param enablePausing if true, pause is enabled. Otherwise disabled.
     */
    void pauseLoopExecution(final boolean enablePausing) {
        if (getInternalState().isExecutionInProgress()) {
            getNode().setPauseLoopExecution(enablePausing);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean isModelCompatibleTo(final Class<?> nodeModelClass) {
        return this.getNode().isModelCompatibleTo(nodeModelClass);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isInactive() {
        return m_node.isInactive();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isInactiveBranchConsumer() {
        return m_node.isInactiveBranchConsumer();
    }

    /* ------------------- Load & Save ---------------- */

    /** {@inheritDoc} */
    @Override
    WorkflowCopyContent performLoadContent(final SingleNodeContainerPersistor nodePersistor,
        final Map<Integer, BufferedDataTable> tblRep, final FlowObjectStack inStack, final ExecutionMonitor exec,
        final LoadResult loadResult, final boolean preserveNodeMessage) throws CanceledExecutionException {
        if (nodePersistor.getMetaPersistor().getState().equals(InternalNodeContainerState.EXECUTED)) {
            m_node.putOutputTablesIntoGlobalRepository(getParent().getGlobalTableRepository());
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void loadExecutionResult(
            final NodeContainerExecutionResult execResult,
            final ExecutionMonitor exec, final LoadResult loadResult) {
        synchronized (m_nodeMutex) {
            if (InternalNodeContainerState.EXECUTED.equals(getInternalState())) {
                LOGGER.debug(getNameWithID()
                        + " is alredy executed; won't load execution result");
                return;
            }
            if (!(execResult instanceof SingleNodeContainerExecutionResult)) {
                throw new IllegalArgumentException("Argument must be instance "
                        + "of \"" + SingleNodeContainerExecutionResult.
                        class.getSimpleName() + "\": "
                        + execResult.getClass().getSimpleName());
            }
            super.loadExecutionResult(execResult, exec, loadResult);
            SingleNodeContainerExecutionResult sncExecResult =
                (SingleNodeContainerExecutionResult)execResult;
            NodeExecutionResult nodeExecResult =
                sncExecResult.getNodeExecutionResult();
            boolean success = sncExecResult.isSuccess();
            if (success) {
                NodeContext.pushContext(this);
                try {
                    m_node.loadDataAndInternals(nodeExecResult, new ExecutionMonitor(), loadResult);
                } finally {
                    NodeContext.removeLastContext();
                }
            }
            boolean needsReset = nodeExecResult.needsResetAfterLoad();
            if (!needsReset && success) {
                for (int i = 0; i < getNrOutPorts(); i++) {
                    if (m_node.getOutputObject(i) == null) {
                        loadResult.addError("Output object at port " + i + " is null");
                        needsReset = true;
                    }
                }
            }
            if (needsReset) {
                execResult.setNeedsResetAfterLoad();
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public SingleNodeContainerExecutionResult createExecutionResult(
            final ExecutionMonitor exec) throws CanceledExecutionException {
        synchronized (m_nodeMutex) {
            SingleNodeContainerExecutionResult result = new SingleNodeContainerExecutionResult();
            super.saveExecutionResult(result);
            NodeContext.pushContext(this);
            try {
                result.setNodeExecutionResult(m_node.createNodeExecutionResult(exec));
            } finally {
                NodeContext.removeLastContext();
            }
            return result;
        }
    }

    /** {@inheritDoc} */
    @Override
    void performSaveModelSettingsTo(final NodeSettings modelSettings) {
        getNode().saveModelSettingsTo(modelSettings);
    }

    /** {@inheritDoc} */
    @Override
    boolean performAreModelSettingsValid(final NodeSettingsRO modelSettings) {
        return getNode().areSettingsValid(modelSettings);
    }

    /* ------------ Stacks and Co --------------- */

    /** {@inheritDoc} */
    @Override
    void setFlowObjectStack(final FlowObjectStack st, final FlowObjectStack outgoingStack) {
        synchronized (m_nodeMutex) {
            pushNodeDropDirURLsOntoStack(st);
            m_node.setFlowObjectStack(st, outgoingStack);
        }
    }

    /** Support old-style drop dir mechanism - replaced since 2.8 with relative mountpoints,
     * e.g. knime://knime.workflow */
    private void pushNodeDropDirURLsOntoStack(final FlowObjectStack st) {
        ReferencedFile refDir = getNodeContainerDirectory();
        ReferencedFile dropFolder = refDir == null ? null
                : new ReferencedFile(refDir, DROP_DIR_NAME);
        if (dropFolder == null) {
            return;
        }
        dropFolder.lock();
        try {
            File directory = dropFolder.getFile();
            if (!directory.exists()) {
                return;
            }
            String[] files = directory.list();
            if (files != null) {
                StringBuilder debug = new StringBuilder(
                        "Found " + files.length + " node local file(s) to "
                        + getNameWithID() + ": ");
                debug.append(Arrays.toString(Arrays.copyOf(files, Math.max(3, files.length))));
                for (String f : files) {
                    File child = new File(directory, f);
                    try {
                        st.push(new FlowVariable(
                                Scope.Local.getPrefix() + "(drop) " + f,
//                                child.getAbsolutePath(), Scope.Local));
                                child.toURI().toURL().toString(), Scope.Local));
//                    } catch (Exception mue) {
                    } catch (MalformedURLException mue) {
                        LOGGER.warn("Unable to process drop file", mue);
                    }
                }
            }
        } finally {
            dropFolder.unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public FlowObjectStack getFlowObjectStack() {
        synchronized (m_nodeMutex) {
            return m_node.getFlowObjectStack();
        }
    }

    /** {@inheritDoc} */
    @Override
    public FlowObjectStack getOutgoingFlowObjectStack() {
        synchronized (m_nodeMutex) {
            return m_node.getOutgoingFlowObjectStack();
        }
    }

    /** {@inheritDoc} */
    @Override
    void performSetCredentialsProvider(final CredentialsProvider cp) {
        m_node.setCredentialsProvider(cp);
    }

    /** {@inheritDoc} */
    @Override
    CredentialsProvider getCredentialsProvider() {
        return m_node.getCredentialsProvider();
    }


    /** Get the tables that are kept by the underlying node. The return value
     * is null if (a) the underlying node is not a
     * {@link org.knime.core.node.BufferedDataTableHolder} or (b) the node
     * is not executed.
     * @return The internally held tables.
     * @see org.knime.core.node.BufferedDataTableHolder
     * @see Node#getInternalHeldTables()
     */
    public BufferedDataTable[] getInternalHeldTables() {
        return getNode().getInternalHeldTables();
    }

    /**
     * Overridden to also ensure that outport tables are "open" (node directory
     * is deleted upon save() - so the tables are better copied into temp).
     * {@inheritDoc}
     */
    @Override
    public void setDirty() {
        /*
         * Ensures that any port object in the associated node is read from its
         * saved location. Especially BufferedDataTable objects are read as late
         * as possible (in order to reduce start-up time), this method makes
         * sure that they are read (and either copied into TMP or into memory),
         * so the underlying node directory can be safely deleted.
         */
        // if-statement fixes bug 1777: ensureOpen can cause trouble if there
        // is a deep hierarchy of BDTs
        if (!isDirty()) {
            NodeContext.pushContext(this);
            try { // only for node context push
                try {
                    m_node.ensureOutputDataIsRead();
                } catch (Exception e) {
                    LOGGER.error("Unable to read output data", e);
                }
                IFileStoreHandler fileStoreHandler = m_node.getFileStoreHandler();
                if (fileStoreHandler instanceof IWriteFileStoreHandler) {
                    try {
                        ((IWriteFileStoreHandler)fileStoreHandler).ensureOpenAfterLoad();
                    } catch (IOException e) {
                        LOGGER.error("Unable to open file store handler " + fileStoreHandler, e);
                    }
                }
            } finally {
                NodeContext.removeLastContext();
            }
        }
        super.setDirty();
    }

    /** {@inheritDoc} */
    @Override
    protected NodeContainerPersistor getCopyPersistor(
            final HashMap<Integer, ContainerTable> tableRep,
            final FileStoreHandlerRepository fileStoreHandlerRepository,
            final boolean preserveDeletableFlags,
            final boolean isUndoableDeleteCommand) {
        return new CopyNativeNodeContainerPersistor(this,
                preserveDeletableFlags, isUndoableDeleteCommand);
    }

    /* ------------------ Node Properties ---------------- */

    /** {@inheritDoc} */
    @Override
    public String getName() {
        return m_node.getName();
    }

    /** {@inheritDoc} */
    @Override
    public NodeType getType() {
        return m_node.getType();
    }

    /** {@inheritDoc} */
    @Override
    public URL getIcon() {
        return m_node.getFactory().getIcon();
    }

    /** {@inheritDoc} */
    @Override
    public Element getXMLDescription() {
        return m_node.getXMLDescription();
    }


    /* ------------------ Dialog ------------- */

    /** {@inheritDoc} */
    @Override
    public boolean hasDialog() {
        return m_node.hasDialog();
    }

    /** {@inheritDoc} */
    @Override
    public final boolean hasDataAwareDialogPane() {
        NodeContext.pushContext(this);
        try {
            return m_node.hasDialog() && (m_node.getDialogPane() instanceof DataAwareNodeDialogPane);
        } finally {
            NodeContext.removeLastContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    NodeDialogPane getDialogPaneWithSettings(final PortObjectSpec[] inSpecs,
            final PortObject[] inData) throws NotConfigurableException {
        NodeSettings settings = new NodeSettings(getName());
        saveSettings(settings, true);
        NodeContext.pushContext(this);
        try {
            return m_node.getDialogPaneWithSettings(inSpecs, inData, settings, getParent().isWriteProtected());
        } finally {
            NodeContext.removeLastContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    NodeDialogPane getDialogPane() {
        NodeContext.pushContext(this);
        try {
            return m_node.getDialogPane();
        } finally {
            NodeContext.removeLastContext();
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean areDialogAndNodeSettingsEqual() {
        final String key = "snc_settings";
        NodeSettingsWO nodeSettings = new NodeSettings(key);
        saveSettings(nodeSettings, true);
        NodeSettingsWO dlgSettings = new NodeSettings(key);
        NodeContext.pushContext(this);
        try {
            m_node.getDialogPane().finishEditingAndSaveSettingsTo(dlgSettings);
        } catch (InvalidSettingsException e) {
            return false;
        } finally {
            NodeContext.removeLastContext();
        }
        return dlgSettings.equals(nodeSettings);
    }

    /* --------------- Output Port Information ---------------- */

    /**
     * {@inheritDoc}
     */
    @Override
    public PortType getOutputType(final int portIndex) {
        return getNode().getOutputType(portIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PortObjectSpec getOutputSpec(final int portIndex) {
        return getNode().getOutputSpec(portIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PortObject getOutputObject(final int portIndex) {
        return getNode().getOutputObject(portIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getOutputObjectSummary(final int portIndex) {
        return getNode().getOutputObjectSummary(portIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HiLiteHandler getOutputHiLiteHandler(final int portIndex) {
        return getNode().getOutputHiLiteHandler(portIndex);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WebTemplate getWebTemplate() {
        // TODO Auto-generated method stub
        return null;
    }


}