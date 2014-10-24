/* $This file is distributed under the terms of the license in /doc/license.txt$ */

package edu.cornell.mannlib.vitro.webapp.servlet.setup.rdfsetup;

import javax.servlet.http.HttpServletRequest;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.ModelMaker;

import edu.cornell.mannlib.vitro.webapp.modelaccess.ModelAccess.WhichService;
import edu.cornell.mannlib.vitro.webapp.modelaccess.ontmodels.OntModelCache;
import edu.cornell.mannlib.vitro.webapp.rdfservice.RDFService;

/**
 * Provides the long-term data structures, and a way to obtain the short-term
 * data structures.
 * 
 * Repeated calls for the same data structure should yield the same instance.
 * 
 * Repeated calls for the ShortTermDataStructuresProvider need not yield the
 * same instance, but must yield an instance that will return the same
 * structures as any other instance for the same request.
 */
public interface DataStructuresProvider {
	RDFService getRDFService(WhichService whichService);

	Dataset getDataset(WhichService whichService);

	ModelMaker getModelMaker(WhichService whichService);

	OntModelCache getOntModelCache();

	ShortTermDataStructuresProvider getShortTermDataStructuresProvider(
			HttpServletRequest req);
}