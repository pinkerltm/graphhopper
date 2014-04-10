/*
 * Copyright 2013 Juergen Zornig.
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

package com.graphhopper.reader.idf;

import com.graphhopper.routing.util.EncodingManager;
import com.graphhopper.storage.DAType;
import com.graphhopper.storage.Directory;
import com.graphhopper.storage.GraphBuilder;
import com.graphhopper.storage.GraphStorage;
import com.graphhopper.storage.MMapDirectory;
import com.graphhopper.storage.index.LocationIndex;
import com.graphhopper.storage.index.LocationIndexTree;
import com.graphhopper.util.EdgeIteratorState;
import com.graphhopper.util.PointList;
import com.graphhopper.util.StopWatch;
import java.io.*;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class parses a CSV File in INTREST idf Format and creates a graph from it. It does so by
 * parsing each single line, distinguishing between metadata lines and data records. Nodes and Edges
 * are prestored in a Hashmap, so that each attributes from the csv tables can be attached while 
 * reading the whole file.
 * <p/>
 * @author Juergen Zornig
 */
public class IdfReader
{
    private final Logger logger = LoggerFactory.getLogger(getClass());
    
    private FileInputStream fis;
    private DataInputStream in;
    private BufferedReader br;
    private GraphStorage graph;
    private HashMap<String, Integer> nodeIdMap;
    private HashMap<String, EdgeIteratorState> edgeMap;
    
    protected final Directory location;
    private final static EncodingManager encodingManager = new EncodingManager("CAR");
    
    private final DAType dataAccessType = new DAType(DAType.RAM_STORE, true);
    private LocationIndex locationIndex;
    private final int snapResolution = 50;
    private final int rowsToCommit = 2;
        
    private enum ItemType {
        Node,
        Link,
        LinkCoordinate,
        LinkUse,
        BikeHike,
        TurnEdge,
        TurnUse,
        Event,
        StreetNames,
        Link2StreetNames
    }
    
    public GraphStorage getGraph() throws IOException {
        
        if(graph == null)
            throw new IllegalStateException("Graph not initialized");
        
        return graph;
    }
    
    public LocationIndex getLocationIndex() {
        if(locationIndex == null)
            throw new IllegalStateException("Location index not initialized");
        
        return locationIndex;
    }
        
    public void doIdf2Graph(String IdfFile) throws IOException
    {
        if (encodingManager == null)
            throw new IllegalStateException("Encoding manager not set.");

        StopWatch sw1 = new StopWatch().start();
        open(IdfFile);
        loadGraph();
        sw1.stop();
        
        StopWatch sw2 = new StopWatch().start();
        initLocationIndex();  
        sw2.stop();
        
        logger.info("INTREST import took " + (int) sw1.getSeconds() + " seconds.");
        logger.info("Location Index built in " + (int) sw2.getSeconds() + " seconds.");
        logger.info("Total seconds: " + (int) (sw1.getSeconds() + sw2.getSeconds()));
    }
    
    public IdfReader(GraphStorage storage) throws IOException {
        
        nodeIdMap = new HashMap<String, Integer>();
        edgeMap = new HashMap<String, EdgeIteratorState>();
        
        graph = storage;
        location = storage.getDirectory();
    }
    
    private void open(String filename) throws IOException {
        
        fis = new FileInputStream(filename);
        in = new DataInputStream(fis);
        br = new BufferedReader(new InputStreamReader(in));
                
    }
    
    private void initLocationIndex()
    {
        locationIndex = new LocationIndexTree(graph, location);
        locationIndex.setResolution(snapResolution).prepareIndex();
        locationIndex.flush();
    }
    
    private void loadGraph() throws IOException {
        String line;
        boolean readsData = false;
        ItemType table = null;
        String[] splitLine;    
        int rowcount = 0;
        PointList edgeVertices = new PointList();
        String edgeKey = "";
    
        //GraphBuilder gb = new GraphBuilder(encodingManager).setLocation(location).setMmap(false);
      
        //GHDirectory dir = new GHDirectory(location, dataAccessType);
        
        //graph = gb.create();
        
        //Read File
        while ((line = br.readLine()) != null) {

            if(!readsData && line.endsWith("tbl;Node")) {
                //Reached Node Table Section
                table = ItemType.Node;
                logger.info("Starting to read nodes!");
                readsData = true;
                
            } else if (!readsData && line.endsWith("tbl;Link")) {
                //Reached Link Table Section
                table = ItemType.Link;
                logger.info("Graph has ".concat(String.valueOf(graph.getNodes())).concat(" nodes."));
                logger.info("Starting to read links!");
                readsData = true;
                
            } else if (!readsData && line.endsWith("tbl;LinkCoordinate")) {
                //Reached LinkCoordinate Table Section
                table = ItemType.LinkCoordinate;
                logger.info("Starting to read LinkCoordinates");
                readsData = true;
                
            } else if (!readsData && line.endsWith("tbl;LinkUse")) {
                //Reached LinkUse Table Section
                table = ItemType.LinkUse;
                logger.info("Starting to read LinkUses");
                readsData = true;
                
            } else if (!readsData && line.endsWith("tbl;BikeHike")) {
                //Reached LinkUse Table Section
                table = ItemType.BikeHike;
                logger.info("Starting to read BikeHike");
                readsData = true;
                
            } else if (!readsData && line.endsWith("tbl;TurnEdge")) {
                //Reached LinkUse Table Section
                table = ItemType.TurnEdge;
                logger.info("Starting to read TurnEdge");
                readsData = true;
                
            } else if (!readsData && line.endsWith("tbl;TurnUse")) {
                //Reached TurnUse Table Section
                table = ItemType.TurnUse;
                logger.info("Starting to read TurnUse");
                readsData = true;
                
            } else if (!readsData && line.endsWith("tbl;Event")) {
                //Reached Event Table Section
                table = ItemType.Event;
                logger.info("Starting to read Events");
                readsData = true;
                
            } else if (!readsData && line.endsWith("tbl;StreetNames")) {
                //Reached StreetNames Table Section
                table = ItemType.StreetNames;
                logger.info("Starting to read StreetNames");
                readsData = true;
                
            } else if (!readsData && line.endsWith("tbl;Link2StreetNames")) {
                //Reached Link2StreetNames Table Section
                table = ItemType.Link2StreetNames;
                logger.info("Starting to read Link2StreetNames");
                readsData = true;

            } else if (readsData && table == ItemType.Node && line.startsWith("rec;")) {
                
                splitLine = line.split(";");
                //splitLine[1]; //NODE_ID
                //splitLine[2]; //LEVEL
                //splitLine[3]; //VIRTUAL_TYPE
                //splitLine[4]; //X
                //splitLine[5]; //Y
                //splitLine[6]; //VIRT_LINKID
                //splitLine[7]; //VIRT_PERCENT
                //splitLine[8]; //BIKE_DELAY
                //splitLine[9]; //STATUS
                
                graph.setNode(
                        rowcount,                           //internal NODEID
                        Double.parseDouble(splitLine[4]),   //LON
                        Double.parseDouble(splitLine[5]));  //LAT
                
                nodeIdMap.put(splitLine[1], rowcount);      //store NodeId for Lookup
                rowcount++;
                
            } else if (readsData && table == ItemType.Link && line.startsWith("rec;")) {
                
                splitLine = line.split(";");
                //splitLine[1]; //LINK_ID
                //splitLine[2]; //NAME1
                //splitLine[3]; //NAME2
                //splitLine[4]; //FROM_NODE
                //splitLine[5]; //TO_NODE
                //splitLine[6]; //SPEED_TOW_CAR
                //splitLine[7]; //SPEED_BKW_CAR
                //splitLine[8]; //SPEED_TOW_TRUCK
                //splitLine[9]; //SPEED_BKW_TRUCK
                //splitLine[10]; //MAXSPEED_TOW_CAR
                //splitLine[11]; //MAXSPEED_BKW_CAR
                //splitLine[12]; //MAXSPEED_TOW_TRUCK
                //splitLine[13]; //MAXSPEED_BKW_TRUCK
                //splitLine[14]; //ACCESS_TOW
                //splitLine[15]; //ACCESS_BKW
                //splitLine[16]; //LENGTH
                //splitLine[17]; //FUNCROADCLASS
                //splitLine[18]; //CAP_TOW
                //splitLine[19]; //CAP_BKW
                //splitLine[20]; //LANES_TOW
                //splitLine[21]; //LANES_BKW
                //splitLine[22]; //FORMOFWAY
                //splitLine[23]; //BRUNNEL
                //splitLine[24]; //MAXHEIGHT
                //splitLine[25]; //MAXWIDTH
                //splitLine[26]; //MAXPRESSURE
                //splitLine[27]; //ABUTTER_CAR
                //splitLine[28]; //ABUTTER_LORRY
                //splitLine[29]; //U_TURN
                //splitLine[30]; //SLOPE
                //splitLine[31]; //URBAN
                //splitLine[32]; //WIDTH
                //splitLine[33]; //LEVEL
                //splitLine[34]; //BAUSTATUS
                //splitLine[35]; //PTV_TYPENO
                //splitLine[36]; //SUBNET_ID
                //splitLine[37]; //ONEWAY
                //splitLine[38]; //BLT
                //splitLine[39]; //BLB
                //splitLine[40]; //EDGE_ID
                //splitLine[41]; //STREETCAT
                //splitLine[42]; //AGG_TYP
                //splitLine[43]; //STATUS
                
                // Make a weighted edge between two nodes. False means the edge is directed.
                //graph.edge(fromId, toId, cost, false);
                try {
                    EdgeIteratorState e = graph.edge(
                            nodeIdMap.get(splitLine[4]), //FROMNODE (internal node Id)
                            nodeIdMap.get(splitLine[5])); //TONODE (internal node Id)
                            
                    e.setDistance(Double.parseDouble(splitLine[16])); //LENGTH
                    
                    e.setName(splitLine[2]);
                     
                    // Keep Edge for latter flagEncoding
                    edgeMap.put(splitLine[1], e);
                    
                    
                } catch(NullPointerException ex) {
                    logger.warn("Either Node Id " + splitLine[4] + " or " + splitLine[5] + " connected by Edge Id " + splitLine[1] + " not found");
                }
                                
                rowcount++;
            
            } else if (readsData && table == ItemType.LinkCoordinate && line.startsWith("rec;")) {
                
                splitLine = line.split(";");
                // These are Vertices of a Link
                //splitLine[1]; //LINK_ID
                //splitLine[2]; //COUNT
                //splitLine[3]; //X
                //splitLine[4]; //Y
                //splitLine[5]; //STATUS
                
                // New Edge Geometry begun?
                if(!splitLine[1].equals(edgeKey)) {
                    // Write geometry to edge point list
                    if(edgeMap.containsKey(edgeKey)) 
                        edgeMap.get(edgeKey).setWayGeometry(edgeVertices);
                                        
                    edgeKey = splitLine[1];
                    edgeVertices.clear();
                }
                
                edgeVertices.add(
                        Double.parseDouble(splitLine[3]),  
                        Double.parseDouble(splitLine[4])
                );
                
                rowcount++;
                
            } else if (readsData && table == ItemType.LinkUse && line.startsWith("rec;")) {
                
                splitLine = line.split(";");
                //splitLine[1]; //USE_ID
                //splitLine[2]; //LINK_ID
                //splitLine[3]; //COUNT
                //splitLine[4]; //OFFSET
                //splitLine[5]; //WIDTH
                //splitLine[6]; //MINWIDTH
                //splitLine[7]; //FROM_PERCENT
                //splitLine[8]; //TO_PERCENT
                //splitLine[9]; //BASETYPE
                //splitLine[10]; //USE_ACCESS_TOW
                //splitLine[11]; //USE_ACCESS_BKW
                //splitLine[12]; //STATUS
                
                //TODO: Do something with the LinkUse data
                
                rowcount++;
                
            } else if (readsData && table == ItemType.BikeHike && line.startsWith("rec;")) {
                
                splitLine = line.split(";");
                
                //splitLine[1]; //USE_ID
                //splitLine[2]; //USE_ACCESS_TOW
                //splitLine[3]; //USE_ACCES_BKW
                //splitLine[4]; //BIKEENVIRONMENT
                //splitLine[5]; //BIKEQUALITYTOW
                //splitLine[6]; //BIKEQUALITYBKW
                //splitLine[7]; //BIKEDIRECTTOW
                //splitLine[8]; //BIKEDIRECTBKW
                //splitLine[9]; //BIKESIGNEDTOW
                //splitLine[10]; //BIKESIGNEDBKW
                //splitLine[11]; //BIKERECOMMTOW
                //splitLine[12]; //BIKERECOMMBKW
                //splitLine[13]; //BIKEWITHFOOTTOW
                //splitLine[14]; //BIKEWITHFOOTBKW
                //splitLine[15]; //STATUS
                //splitLine[16]; //BIKEFEATURETOW
                //splitLine[17]; //BIKEFEATUREBKW
                
                
                //TODO: Do something with the BikeHike data
                
                rowcount++;
                
            } else if (readsData && table == ItemType.TurnEdge && line.startsWith("rec;")) {
                
                splitLine = line.split(";");
                
                //splitLine[1]; //TURN_ID
                //splitLine[2]; //FROM_LINK
                //splitLine[3]; //TO_LINK
                //splitLine[4]; //VIA_NODE
                //splitLine[5]; //VEHICLE_TYPE
                //splitLine[6]; //TIME
                //splitLine[7]; //CAPACITY
                //splitLine[8]; //LANESFROM
                //splitLine[9]; //LANESTO
                //splitLine[10]; //STATUS
                
                //TODO: Do something with the TurnEdge data
                
                rowcount++;
            
            } else if (readsData && table == ItemType.TurnUse && line.startsWith("rec;")) {
                
                splitLine = line.split(";");
                
                //splitLine[1]; //TURN_ID
                //splitLine[2]; //FROM_USE
                //splitLine[3]; //TO_USE
                //splitLine[4]; //FROM_PERCENT
                //splitLine[5]; //TO_PERCENT
                //splitLine[6]; //VEHICLE_TYPE
                //splitLine[7]; //TIME
                //splitLine[8]; //TURN_TYPE
                //splitLine[9]; //TURN_DIRECTION_FROM
                //splitLine[10]; //TURN_DIRECTION_TO
                //splitLine[11]; //BASETYPE
                //splitLine[12]; //STATUS
                
                //TODO: Do something with the TurnUse data
                
                rowcount++;
                
            } else if (readsData && table == ItemType.Event && line.startsWith("rec;")) {

                splitLine = line.split(";");
                
                //splitLine[1]; //OBJ_TYPE
                //splitLine[2]; //OBJ_ID
                //splitLine[3]; //VEHICLETYPE
                //splitLine[4]; //RESTRICTIONTYPE
                //splitLine[5]; //RESTRICTIONVALUE
                //splitLine[6]; //PERIODTYPE
                //splitLine[7]; //DATEFROM
                //splitLine[8]; //DATETO
                //splitLine[9]; //TIMEFROM
                //splitLine[10]; //TIMETO
                //splitLine[11]; //STATUS
                
                //TODO: Do something with the Event data
                
                rowcount++;
            
            } else if (readsData && table == ItemType.StreetNames && line.startsWith("rec;")) {

                splitLine = line.split(";");
                
                //splitLine[1]; //OFFICIAL_MUN_CODE
                //splitLine[2]; //STREET_ID
                //splitLine[3]; //STREET_NAME
                //splitLine[4]; //PARTOFPLACE_NAME
                //splitLine[5]; //STATUS
                
                rowcount++;

            } else if (readsData && table == ItemType.Link2StreetNames && line.startsWith("rec;")) {

                splitLine = line.split(";");
                
                //splitLine[1]; //LINK_ID
                //splitLine[2]; //COUNT
                //splitLine[3]; //OFFICAL_MUN_CODE
                //splitLine[4]; //STREET_ID
                //splitLine[5]; //STATUS
                
                rowcount++;

            } else if (readsData && line.startsWith("end;")) {
                
                splitLine = line.split(";");
                
                if(rowcount != Integer.parseInt(splitLine[1])) {
                    logger.warn(String.format("Read %d records, but should be %s", rowcount, splitLine[1]));
                } else {
                    logger.info(String.format("Read %d records which equals %s rows as expected", rowcount, splitLine[1]));
                }
                
                //Finish linkcoordinates reading
                //TODO: Do this somewhere else
                if(table == ItemType.LinkCoordinate) {
                    EdgeIteratorState e = edgeMap.get(edgeKey);
                    e.setWayGeometry(edgeVertices);
                    
                    edgeVertices.clear();
                }
                
                rowcount = 0;                
                table = null;
                readsData = false;
                
            }         
            
            if((rowcount > 0) && ((rowcount % rowsToCommit) == 0)) {
                logger.info("Read ".concat(String.valueOf(rowcount)).concat(" lines so far."));
                graph.flush();
            }
        }          

        graph.flush();
                
        //Close the input stream
        in.close();
    }
    

    void close()
    {
        if (graph != null){
            graph.flush();
            graph.close();
        }

        if (locationIndex != null)
            locationIndex.close();
    }
}
