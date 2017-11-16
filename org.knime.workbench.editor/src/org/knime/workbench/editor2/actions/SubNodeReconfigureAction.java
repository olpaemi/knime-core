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
 *   12.05.2010 (Bernd Wiswedel): created
 *   21.06.2012 (Peter Ohl): reconfigure instead of rename only. Using a wizard dialog.
 */
package org.knime.workbench.editor2.actions;

import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.swt.widgets.Display;
import org.knime.core.node.workflow.SubNodeContainer;
import org.knime.core.node.workflow.WorkflowManager;
import org.knime.workbench.KNIMEEditorPlugin;
import org.knime.workbench.core.util.ImageRepository;
import org.knime.workbench.editor2.WorkflowEditor;
import org.knime.workbench.editor2.editparts.NodeContainerEditPart;
import org.knime.workbench.editor2.subnode.SetupSubnodeWizard;
import org.knime.workbench.editor2.subnode.SubnodeWizardDialog;

/**
 *
 * @author Patrick Winter, KNIME AG, Zurich, Switzerland
 */
public class SubNodeReconfigureAction extends AbstractNodeAction {

    /** id of this action. */
    public static final String ID = "knime.action.sub_node_reconfigure";

    /**
     * @param editor the current workflow editor
     */
    public SubNodeReconfigureAction(final WorkflowEditor editor) {
        super(editor);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return ID;
    }

    /**
     *
     * {@inheritDoc}
     */
    @Override
    public String getText() {
        return "Setup...";
    }

    /**
     *
     * {@inheritDoc}
     */
    @Override
    public String getToolTipText() {
        return "Change name, ports and behaviour of Wrapped Metanode";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ImageDescriptor getImageDescriptor() {
        return ImageRepository.getIconDescriptor(KNIMEEditorPlugin.PLUGIN_ID, "icons/meta/metanode_setname.png");
    }

    /**
     * @return true, if underlying model instance of <code>WorkflowManager</code>, otherwise false
     */
    @Override
    protected boolean internalCalculateEnabled() {
        final WorkflowManager manager = getManager();
        if(manager == null) {
            return false;
        }
        if (manager.isWriteProtected()) {
            return false;
        }
        if (manager.getDirectNCParent() instanceof SubNodeContainer) {
            return true;
        }
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public void runOnNodes(final NodeContainerEditPart[] nodeParts) {
        SubNodeContainer subnode = (SubNodeContainer)getManager().getDirectNCParent();
        NodeContainerEditPart[] editorParts = getAllParts(NodeContainerEditPart.class);
        if (editorParts.length <= 0) {
            return;
        }
        SetupSubnodeWizard wizard = new SetupSubnodeWizard(editorParts[0].getParent().getViewer(), subnode);
        SubnodeWizardDialog dlg = new SubnodeWizardDialog(Display.getCurrent().getActiveShell(), wizard);
        dlg.create();
        dlg.open();
    }

}
