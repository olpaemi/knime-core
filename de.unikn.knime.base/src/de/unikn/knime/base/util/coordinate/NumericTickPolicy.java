/* 
 * -------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 * 
 * Copyright, 2003 - 2006
 * Universitaet Konstanz, Germany.
 * Lehrstuhl fuer Angewandte Informatik
 * Prof. Dr. Michael R. Berthold
 * 
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner.
 * -------------------------------------------------------------------
 * 
 * History
 *   02.02.2006 (sieb): created
 */
package de.unikn.knime.base.util.coordinate;

/**
 * Enumeration of all available policies determine the position of ticks of a
 * {@link de.unikn.knime.base.util.coordinate.NumericCoordinate}.
 * 
 * @author Christoph Sieb, University of Konstanz
 */
public enum NumericTickPolicy {

    /**
     * The policy which sets a tick at the first and last domain value. In
     * between the ticks are distributed equally with a minimum distance defined
     * in {@link NumericCoordinate#DEFAULT_ABSOLUTE_TICK_DIST}.
     */
    START_WITH_FIRST_END_WITH_LAST_DOMAINE_VALUE,

    /**
     * Sets the ticks such that the lables are rounded according to the domain
     * range.
     */
    LABLE_WITH_ROUNDED_NUMBERS
}
