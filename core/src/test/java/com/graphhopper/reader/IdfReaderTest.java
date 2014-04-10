/*
 * Copyright 2014 root.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.graphhopper.reader;

import com.graphhopper.GraphHopper;
import com.graphhopper.reader.OSMReaderTest.GraphHopperTest;
import com.graphhopper.reader.idf.IdfReader;
import com.graphhopper.routing.util.*;
import com.graphhopper.storage.*;
import com.graphhopper.storage.index.LocationIndex;
import com.graphhopper.util.EdgeExplorer;
import com.graphhopper.util.Helper;
import com.graphhopper.util.shapes.BBox;
import java.io.File;
import java.io.IOException;
import org.junit.*;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Jürgen Zornig
 */
public class IdfReaderTest {
    
        private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private final String file1 = "./files/test-intrest1.txt";
    private final String file2 = "./files/test-intrest2.txt";
    private final String file3 = "./files/test-intrest3.txt";
    //private final String file4 = "./files/IntrestExport_test.txt";
    //private final String dir = "./target/tmp";
    private final String dir = "./target/tmp/test-db";
    private CarFlagEncoder carEncoder;
    private FootFlagEncoder footEncoder;
    private EdgeExplorer carOutExplorer;
    private EdgeExplorer carAllExplorer;
    
    private EncodingManager encodingManager = new EncodingManager("CAR");
    private EdgeFilter carOutEdges = new DefaultEdgeFilter(encodingManager.getEncoder("CAR"), false, true);
    
    @Before
    public void setUp() {
        new File(dir).mkdirs();
    }

    @After
    public void tearDown() {
        Helper.removeDir(new File(dir));
    }
    
    GraphStorage buildGraph( String directory, EncodingManager encodingManager )
    {
        return new GraphHopperStorage(new RAMDirectory(directory, false), encodingManager);
    }
    
    @Test
    public void testLoadFile() throws IOException {
        logger.info("start creating graph from " + file1);
        IdfReader reader = new IdfReader(dir);
       
        /*OSMReader reader = new OSMReader(graph, expectedCapacity).
                setWorkerThreads(workerThreads).
                setEncodingManager(encodingManager).
                setWayPointMaxDistance(wayPointMaxDistance).
                setEnableInstructions(enableInstructions);*/
        logger.info("using " + dir.toString() + ", memory:" + Helper.getMemInfo());
        reader.doIdf2Graph(file1);
       Graph g = idfRead.getGraph();
       assertEquals(922, g.getNodes());
       
       BBox bbox = new BBox(48.5320659, 48.6628279, 15.1048561, 15.2930946);
       assertEquals(bbox, g.getBounds());
       
       LocationIndex li = idfRead.getLocationIndex();
       
    }
    
    @Test
    public void testMain()
    {
        GraphHopper hopper = new GraphHopperTest(file1).importOrLoad();
        Graph graph = hopper.getGraph();
        assertEquals(4, graph.getNodes());
        int n20 = AbstractGraphStorageTester.getIdOf(graph, 52);
        int n10 = AbstractGraphStorageTester.getIdOf(graph, 51.2492152);
        int n30 = AbstractGraphStorageTester.getIdOf(graph, 51.2);
        int n50 = AbstractGraphStorageTester.getIdOf(graph, 49);
        assertEquals(GHUtility.asSet(n20), GHUtility.getNeighbors(carOutExplorer.setBaseNode(n10)));
        assertEquals(3, GHUtility.count(carOutExplorer.setBaseNode(n20)));
        assertEquals(GHUtility.asSet(n20), GHUtility.getNeighbors(carOutExplorer.setBaseNode(n30)));

        EdgeIterator iter = carOutExplorer.setBaseNode(n20);
        assertTrue(iter.next());
        assertEquals("street 123, B 122", iter.getName());
        assertEquals(n50, iter.getAdjNode());
        AbstractGraphStorageTester.assertPList(Helper.createPointList(51.25, 9.43), iter.fetchWayGeometry(0));
        CarFlagEncoder flags = carEncoder;
        assertTrue(flags.isForward(iter.getFlags()));
        assertTrue(flags.isBackward(iter.getFlags()));

        assertTrue(iter.next());
        assertEquals("route 666", iter.getName());
        assertEquals(n30, iter.getAdjNode());
        assertEquals(93147, iter.getDistance(), 1);

        assertTrue(iter.next());
        assertEquals("route 666", iter.getName());
        assertEquals(n10, iter.getAdjNode());
        assertEquals(88643, iter.getDistance(), 1);

        assertTrue(flags.isForward(iter.getFlags()));
        assertTrue(flags.isBackward(iter.getFlags()));
        assertFalse(iter.next());

        // get third added location id=30
        iter = carOutExplorer.setBaseNode(n30);
        assertTrue(iter.next());
        assertEquals("route 666", iter.getName());
        assertEquals(n20, iter.getAdjNode());
        assertEquals(93146.888, iter.getDistance(), 1);

        assertEquals(9.4, graph.getLongitude(hopper.getLocationIndex().findID(51.2, 9.4)), 1e-3);
        assertEquals(10, graph.getLongitude(hopper.getLocationIndex().findID(49, 10)), 1e-3);
        assertEquals(51.249, graph.getLatitude(hopper.getLocationIndex().findID(51.2492152, 9.4317166)), 1e-3);

        // node 40 is on the way between 30 and 50 => 9.0
        assertEquals(9, graph.getLongitude(hopper.getLocationIndex().findID(51.25, 9.43)), 1e-3);
    }
            
    
    
    
}
