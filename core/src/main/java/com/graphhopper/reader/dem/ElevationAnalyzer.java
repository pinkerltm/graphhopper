package com.graphhopper.reader.dem;

import com.graphhopper.reader.GeometryAccess;
import com.graphhopper.reader.OSMWay;
import com.graphhopper.util.DistanceCalc;
import com.graphhopper.util.Helper;
import com.graphhopper.util.PointList;
import gnu.trove.list.TIntList;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TIntArrayList;

/**
 * Analyzes the elevation differences along a way.
 * Author: Nop
 */
public class ElevationAnalyzer
{
    public static final int LAT = 0;
    public static final int LON = 1;
    public static final int ELEVATION = 2;
    public static final int DISTANCE = 3;
    public static final int ANGLE = 4;

    public static final int RECORD_SIZE = 5;

    private static DistanceCalc distCalc = new DistanceCalc();


    private GeometryAccess geometryAccess;

    private int count;
    // list contains lat, lon, elevation and distance
    private TIntList nodes;

    private int ascend;
    private int descend;
    private int ascendDistance;
    private int levelDistance;
    private int descendDistance;
    private int totalDistance;

    private int totalIncline;
    private int avgIncline;
    private int avgDecline;

    private int contAscend;
    private int contAscendDistance;
    private int contDescend;
    private int contDescendDistance;

    private int maxAngle;

    public ElevationAnalyzer( GeometryAccess geometryAccess )
    {
        this.geometryAccess = geometryAccess;
    }

    public ElevationAnalyzer()
    {
    }

    /**
     * Initialize the Analyzer with the way data.
     * Calculate total distance.
     * @param way
     * @param geometryAccess
     */
    public void initialize( OSMWay way, GeometryAccess geometryAccess ) {
        this.geometryAccess = geometryAccess;

        TLongList nodeIds = way.getNodes();
        count = nodeIds.size();
        nodes = new TIntArrayList( 2 * RECORD_SIZE * count );

        totalDistance = 0;

        double lastLat = 0;
        double lastLon = 0;
        double lat = 0;
        double lon = 0;
        int node[] = new int[3];
        for( int i = 0; i < count; i++ ) {
            long osmId = nodeIds.get( i );

            // get node coordinates and elevation from graph
            geometryAccess.getNode( osmId, node );
            lat = Helper.intToDegree(node[0]);
            lon = Helper.intToDegree( node[1] );

            nodes.add( node );

            // calculate distances to previous node
            if( i == 0 )
            {
                nodes.add( 0 );
                nodes.add(0);
            } else
            {
                double distance = distCalc.calcDist( lastLat, lastLon, lat, lon );
                nodes.add( (int) distance );
                nodes.add(0);
                totalDistance += distance;
            }
            lastLat = lat;
            lastLon = lon;
        }
    }

    /**
     * Initialize the Analyzer with a point list.
     *
     * @param points
     */
    public void initialize( PointList points )
    {
        count = points.getSize();
        nodes = new TIntArrayList(2 * RECORD_SIZE * count);

        totalDistance = 0;
        maxAngle = 0;

        double prevLat = 0;
        double prevLon = 0;
        double lastLat = 0;
        double lastLon = 0;
        double lat = 0;
        double lon = 0;

        for (int i = 0; i < count; i++)
        {
            lat = points.getLatitude(i);
            lon = points.getLongitude(i);

            nodes.add(Helper.degreeToInt(lat));
            nodes.add(Helper.degreeToInt(lon));
            nodes.add((int) points.getElevation(i));

            // calculate distances to previous node
            if (i == 0)
            {
                // distance and curve angle
                nodes.add(0);
                nodes.add(0);
            } else
            {
                double distance = distCalc.calcDist(lastLat, lastLon, lat, lon);
                nodes.add((int) distance);
                nodes.add(0);
                totalDistance += distance;
                if( i > 1 )
                {
                    final double ax = (lastLon - prevLon);
                    final double ay = (lastLat - prevLat);
                    final double bx = (lon - lastLon);
                    final double by = (lat - lastLat);

                    int angle = (int) Math.toDegrees(Math.acos((ax * bx + ay * by) / (Math.sqrt(ax * ax + ay * ay) * Math.sqrt(bx * bx + by * by))));
                    nodes.set((i-1)*RECORD_SIZE + ANGLE, angle);
                    maxAngle = Math.max( maxAngle, Math.abs(angle));
                }
            }
            prevLat = lastLat;
            prevLon = lastLon;
            lastLat = lat;
            lastLon = lon;
        }
    }

    public void interpolate( int step )
    {
        int[] node = new int[RECORD_SIZE];

        for( int i = 1; i < count; i++ ) {
            int index = i* RECORD_SIZE;
            int distance = nodes.get( index + DISTANCE );
            int parts = distance / step;
            if( parts > 1 )
            {
                int baseLat = nodes.get( index - RECORD_SIZE + LAT );
                int baseLon = nodes.get( index - RECORD_SIZE + LON );
                int destLat = nodes.get( index + LAT );
                int destLon = nodes.get( index + LON );

                final int part = distance / parts;
                nodes.set( index + DISTANCE, part );
                node[3] = part;
                for( int j=1; j<parts; j++)
                {
                    node[0] = baseLat + j * (destLat-baseLat) / parts;
                    node[1] = baseLon + j * (destLon-baseLon) / parts;
                    node[2] = geometryAccess.getElevation( Helper.intToDegree( node[0]), Helper.intToDegree( node[1]));

                    nodes.insert( index, node );
                    i++;
                    count++;
                    index += RECORD_SIZE;
                }
            }
        }
    }

    public void analyzeElevations()
    {
        // calculate total distance again, might be modified by interpolation rounding errors.
        totalDistance = 0;
        ascend=0;
        descend=0;
        ascendDistance=0;
        descendDistance=0;
        levelDistance=0;
        totalDistance =0;

        contAscend = 0;
        contAscendDistance = 0;
        contDescend = 0;
        contDescendDistance = 0;

        avgIncline=0;
        avgDecline=0;

        int partAscend = 0;
        int partAscendDistance = 0;
        int partDescend = 0;
        int partDescendDistance = 0;

        int lastEle = nodes.get( ELEVATION );
        for (int i = 1; i < count; i++)
        {
            int index = i * RECORD_SIZE;
            int ele = nodes.get(index + ELEVATION);
            int distance = nodes.get(index + DISTANCE);
            totalDistance += distance;
            if (distance > 0)
            {
                int delta = ele - lastEle;
                int incline = 100 * delta / distance;

                if( delta == 0 )
                {
                    levelDistance += distance;

                    partAscendDistance += distance;
                    partDescendDistance += distance;
                } else if (delta > 0)
                {
                    ascend += delta;
                    ascendDistance += distance;

                    partAscend += delta;
                    partAscendDistance += distance;

                    // end of continuous ascend
                    checkContinuousDescend(partDescend, partDescendDistance);
                    partDescend = 0;
                    partDescendDistance = 0;
                }
                if( delta < 0 ) {
                    descend += delta;
                    descendDistance += distance;

                    partDescend += delta;
                    partDescendDistance += distance;

                    // end of continuous ascend
                    checkContinuousAscend(partAscend, partAscendDistance);
                    partAscend = 0;
                    partAscendDistance = 0;
                }

            }
            lastEle = ele;
        }
        checkContinuousAscend(partAscend, partAscendDistance);
        checkContinuousDescend(partDescend, partDescendDistance);

        if (totalDistance > 20)
        {
            totalIncline = 100*(ascend+descend)/totalDistance;
        }
        if (ascendDistance > 20)
        {
            avgIncline = 100*ascend / ascendDistance;
        }
        if (descendDistance > 20)
        {
            avgDecline = 100*descend / descendDistance;
    }
    }

    private void checkContinuousAscend( int partAscend, int partAscendDistance )
    {
        if( partAscendDistance > contAscendDistance )
        {
            contAscend = partAscend;
            contAscendDistance = partAscendDistance;
        }
    }

    private void checkContinuousDescend( int partDescend, int partDescendDistance )
    {
        if( partDescendDistance > contDescendDistance )
        {
            contDescend = partDescend;
            contDescendDistance = partDescendDistance;
        }
    }

    public int getAscend()
    {
        return ascend;
    }

    public int getDescend() {
        return descend;
    }

    public int getTotalDistance() {
        return totalDistance;
    }

    public int getAverageIncline() {
        return avgIncline;
    }

    public int getAscendDistance()
    {
        return ascendDistance;
    }

    public int getDescendDistance() {
        return descendDistance;
    }

    public int getLevelDistance() {
        return levelDistance;
    }

    public int getTotalIncline() {
        return totalIncline;
    }

    public int getAverageDecline() {
        return avgDecline;
    }

    public int getContinuousAscend()
    {
        return contAscend;
    }

    public int getContinuousAscendDistance()
    {
        return contAscendDistance;
    }

    public int getContinuousDescend()
    {
        return contDescend;
    }

    public int getContinuousDescendDistance()
    {
        return contDescendDistance;
    }

    public int getMaxAngle()
    {
        return maxAngle;
    }
}
