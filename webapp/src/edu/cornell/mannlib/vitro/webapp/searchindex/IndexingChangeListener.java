/* $This file is distributed under the terms of the license in /doc/license.txt$ */

package edu.cornell.mannlib.vitro.webapp.searchindex;

import static edu.cornell.mannlib.vitro.webapp.modules.searchIndexer.SearchIndexer.Event.Type.PAUSE;
import static edu.cornell.mannlib.vitro.webapp.modules.searchIndexer.SearchIndexer.Event.Type.START_REBUILD;
import static edu.cornell.mannlib.vitro.webapp.modules.searchIndexer.SearchIndexer.Event.Type.UNPAUSE;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.*;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import edu.cornell.mannlib.vitro.webapp.dao.jena.event.EditEvent;
import edu.cornell.mannlib.vitro.webapp.modules.searchIndexer.SearchIndexer;
import edu.cornell.mannlib.vitro.webapp.modules.searchIndexer.SearchIndexer.Event;
import edu.cornell.mannlib.vitro.webapp.rdfservice.ChangeListener;
import edu.cornell.mannlib.vitro.webapp.utils.threads.VitroBackgroundThread;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotReader;

/**
 * When a change is heard, wait for an interval to see if more changes come in.
 * When changes stop coming in for a specified interval, send what has
 * accumulated.
 * 
 * When the SearchIndexer pauses, stop sending changes until the SearchIndexer
 * unpauses.
 * 
 * If the SearchIndexer begins a rebuild, discard any changes that we had
 * accumulated. They will be accomplished by the rebuild.
 */
public class IndexingChangeListener implements ChangeListener,
		SearchIndexer.Listener {
	private static final Log log = LogFactory
			.getLog(IndexingChangeListener.class);

	private final SearchIndexer searchIndexer;
	private final Ticker ticker;
    private final Model defaultModel;
	private volatile boolean paused = true;

	/** All access to the list must be synchronized. */
	private final List<Statement> changes;

	public IndexingChangeListener(SearchIndexer searchIndexer) {
		this.searchIndexer = searchIndexer;
		this.ticker = new Ticker();
        this.defaultModel = ModelFactory.createDefaultModel();
		this.changes = new ArrayList<>();

		searchIndexer.addListener(this);
	}

	private synchronized void noteChange(Statement stmt) {
        changes.add(stmt);
        if (!paused) {
            ticker.start();
        }
	}

	@Override
	public void receiveSearchIndexerEvent(Event event) {
		if (event.getType() == PAUSE) {
			paused = true;
		} else if (event.getType() == UNPAUSE) {
			paused = false;
			ticker.start();
		} else if (event.getType() == START_REBUILD) {
			discardChanges();
		}
	}

	private synchronized void respondToTicker() {
		if (!paused && !changes.isEmpty()) {
			searchIndexer.scheduleUpdatesForStatements(changes);
			changes.clear();
		}
	}

	private synchronized void discardChanges() {
		changes.clear();
	}

	public void shutdown() {
		ticker.shutdown();
	}

	@Override
	public void addedStatement(String serializedTriple, String graphURI) {
		noteChange(parseTriple(serializedTriple));
	}

	@Override
	public void removedStatement(String serializedTriple, String graphURI) {
		noteChange(parseTriple(serializedTriple));
	}

	/**
	 * We only care about events that signal the end of an edit operation.
	 */
	@Override
	public void notifyEvent(String graphURI, Object event) {
		if ((event instanceof EditEvent)) {
			EditEvent editEvent = (EditEvent) event;
			if (!editEvent.getBegin()) { // editEvent is the end of an edit
				log.debug("Doing search index build at end of EditEvent");
				ticker.start();
			}
		} else {
			log.debug("ignoring event " + event.getClass().getName() + " "
					+ event);
		}
	}

	// TODO avoid duplication with JenaChangeListener
	private Statement parseTriple(String serializedTriple) {
        try {
            Iterator<Triple> it = RiotReader.createIteratorTriples(new ByteArrayInputStream(serializedTriple.getBytes("UTF-8")), RDFLanguages.NTRIPLES, null);

            if (it.hasNext()) {
                Triple triple = it.next();

                if (it.hasNext()) {
                    log.warn("More than one triple parsed from change event");
                }

                // Converts the Triple into a Statement, without attaching it to the Model
                return defaultModel.asStatement(triple);

/*
                -- Alternative way to create a statement, without using a Model
                return ResourceFactory.createStatement(
                        ResourceFactory.createResource(triple.getSubject().getURI()),
                        ResourceFactory.createProperty(triple.getPredicate().getURI()),
                        makeObject(triple.getObject()));
*/
            } else {
                throw new RuntimeException("no triple parsed from change event");
            }
        } catch (RuntimeException riot) {
            log.error("Failed to parse triple " + serializedTriple, riot);
            throw riot;
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException(uee);
        }
	}

    /**
     * If not using Model.asStatement, this helper method is required to convert a Triple to a Statement
     */
    /*
    private RDFNode makeObject(Node object) {
        if (object.isURI()) {
            return ResourceFactory.createResource(object.getURI());
        }
        if (object.isLiteral()) {
            if (StringUtils.isNotEmpty(object.getLiteralLanguage())) {
                return ResourceFactory.createLangLiteral(object.getLiteralValue().toString(), object.getLiteralLanguage());
            }

            if (null != object.getLiteralDatatype()) {
                return ResourceFactory.createTypedLiteral(object.getLiteralValue());
            }

            return ResourceFactory.createPlainLiteral(object.getLiteralValue().toString());
        }

        return ResourceFactory.createResource();
    }
    */

	// ----------------------------------------------------------------------
	// helper classes
	// ----------------------------------------------------------------------

	/**
	 * The ticker will ask for a response after two ticks, unless it is started
	 * again before the second one.
	 * 
	 * <pre>
	 * On a call to start():
	 *    Start the timer unless it is already running.
	 *    Reset the hasOneTick flag.
	 *    
	 * When the timer expires:
	 *    If the timer hasOneTick, we're done: call for a response.
	 *    Otherwise, record that it hasOneTick, and keep the timer running.
	 * </pre>
	 * 
	 * All methods are synchronized on the enclosing IndexingChangeListener.
	 */
	private class Ticker {
		private final ScheduledExecutorService queue;
		private volatile boolean running;
		private volatile boolean hasOneTick;

		public Ticker() {
			this.queue = Executors.newScheduledThreadPool(1,
					new VitroBackgroundThread.Factory(
							"IndexingChangeListener_Ticker"));
		}

		public void shutdown() {
			synchronized (IndexingChangeListener.this) {
				this.queue.shutdown();
			}
		}

		public void start() {
			synchronized (IndexingChangeListener.this) {
				if (!running) {
					startTicker();
				}
				hasOneTick = false;
			}
		}

		private void startTicker() {
			if (queue.isShutdown()) {
				log.warn("Attempt to start ticker after shutdown request.");
			} else {
				queue.schedule(new TickerResponse(), 1, TimeUnit.SECONDS);
				running = true;
			}
		}

		private class TickerResponse implements Runnable {
			@Override
			public void run() {
				synchronized (IndexingChangeListener.this) {
					running = false;
					if (hasOneTick) {
						respondToTicker();
						hasOneTick = false;
					} else {
						startTicker();
						hasOneTick = true;
					}
				}
			}
		}
	}
}
