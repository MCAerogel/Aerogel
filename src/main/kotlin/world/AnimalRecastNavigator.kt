package org.macaroon3145.world

import org.recast4j.detour.DefaultQueryFilter
import org.recast4j.detour.NavMesh
import org.recast4j.detour.NavMeshBuilder
import org.recast4j.detour.NavMeshDataCreateParams
import org.recast4j.detour.NavMeshQuery
import org.recast4j.detour.StraightPathItem
import org.recast4j.recast.AreaModification
import org.recast4j.recast.PolyMesh
import org.recast4j.recast.PolyMeshDetail
import org.recast4j.recast.RecastBuilder
import org.recast4j.recast.RecastBuilderConfig
import org.recast4j.recast.RecastConfig
import org.recast4j.recast.RecastConstants
import org.recast4j.recast.geom.SimpleInputGeomProvider
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.max
import kotlin.math.min

data class AnimalWaypoint(
    val x: Double,
    val y: Double,
    val z: Double
)

class AnimalRecastNavigator(
    private val blockStateAt: (Int, Int, Int) -> Int,
    private val isSolidState: (Int) -> Boolean
) {
    private val queryFilter = DefaultQueryFilter()
    private val groundArea = AreaModification(1)

    fun findNextWaypoint(
        startX: Double,
        startY: Double,
        startZ: Double,
        targetX: Double,
        targetY: Double,
        targetZ: Double,
        agentRadius: Double,
        agentHeight: Double,
        agentMaxClimb: Double
    ): AnimalWaypoint? {
        val bounds = queryBounds(startX, startY, startZ, targetX, targetY, targetZ)
        val geometry = buildSurfaceGeometry(
            minX = bounds.minX,
            maxX = bounds.maxX,
            minY = bounds.minY,
            maxY = bounds.maxY,
            minZ = bounds.minZ,
            maxZ = bounds.maxZ,
            requiredHeight = agentHeight
        )
        if (geometry.faces.isEmpty()) return null

        val geomProvider = SimpleInputGeomProvider(geometry.vertices, geometry.faces)
        val recastCfg = RecastConfig(
            RecastConstants.PartitionType.WATERSHED,
            CELL_SIZE,
            CELL_HEIGHT,
            max(MIN_AGENT_HEIGHT, agentHeight.toFloat()),
            max(MIN_AGENT_RADIUS, agentRadius.toFloat()),
            max(MIN_AGENT_CLIMB, agentMaxClimb.toFloat()),
            MAX_AGENT_SLOPE,
            REGION_MIN_SIZE,
            REGION_MERGE_SIZE,
            EDGE_MAX_LEN,
            EDGE_MAX_ERROR,
            MAX_VERTS_PER_POLY,
            DETAIL_SAMPLE_DIST,
            DETAIL_SAMPLE_MAX_ERROR,
            groundArea
        )

        val recastResult = RecastBuilder().build(
            geomProvider,
            RecastBuilderConfig(recastCfg, geomProvider.meshBoundsMin, geomProvider.meshBoundsMax)
        )
        val polyMesh: PolyMesh = recastResult.mesh ?: return null
        if (polyMesh.npolys <= 0) return null
        val detailMesh: PolyMeshDetail = recastResult.meshDetail ?: return null

        for (i in 0 until polyMesh.npolys) {
            polyMesh.flags[i] = NAV_WALK_FLAG
        }

        val navParams = NavMeshDataCreateParams().apply {
            verts = polyMesh.verts
            vertCount = polyMesh.nverts
            polys = polyMesh.polys
            polyAreas = polyMesh.areas
            polyFlags = polyMesh.flags
            polyCount = polyMesh.npolys
            nvp = polyMesh.nvp

            detailMeshes = detailMesh.meshes
            detailVerts = detailMesh.verts
            detailVertsCount = detailMesh.nverts
            detailTris = detailMesh.tris
            detailTriCount = detailMesh.ntris

            walkableHeight = max(MIN_AGENT_HEIGHT, agentHeight.toFloat())
            walkableRadius = max(MIN_AGENT_RADIUS, agentRadius.toFloat())
            walkableClimb = max(MIN_AGENT_CLIMB, agentMaxClimb.toFloat())
            bmin = polyMesh.bmin
            bmax = polyMesh.bmax
            cs = CELL_SIZE
            ch = CELL_HEIGHT
            buildBvTree = true
        }

        val meshData = NavMeshBuilder.createNavMeshData(navParams) ?: return null
        val navMesh = NavMesh(meshData, polyMesh.nvp, 0)
        val navQuery = NavMeshQuery(navMesh)

        val startPos = floatArrayOf(startX.toFloat(), startY.toFloat(), startZ.toFloat())
        val endPos = floatArrayOf(targetX.toFloat(), targetY.toFloat(), targetZ.toFloat())
        val extents = floatArrayOf(SEARCH_HALF_EXTENT_XZ, SEARCH_HALF_EXTENT_Y, SEARCH_HALF_EXTENT_XZ)

        val nearestStart = navQuery.findNearestPoly(startPos, extents, queryFilter)
        if (nearestStart.failed() || nearestStart.result == null) return null
        val nearestEnd = navQuery.findNearestPoly(endPos, extents, queryFilter)
        if (nearestEnd.failed() || nearestEnd.result == null) return null

        val startRef = nearestStart.result.getNearestRef()
        val endRef = nearestEnd.result.getNearestRef()
        if (startRef == 0L || endRef == 0L) return null

        val nearestStartPos = nearestStart.result.getNearestPos()
        val nearestEndPos = nearestEnd.result.getNearestPos()

        val pathResult = navQuery.findPath(startRef, endRef, nearestStartPos, nearestEndPos, queryFilter)
        if (pathResult.failed() || pathResult.result == null || pathResult.result.isEmpty()) return null

        val straightResult = navQuery.findStraightPath(
            nearestStartPos,
            nearestEndPos,
            pathResult.result,
            MAX_STRAIGHT_PATH_POINTS,
            0
        )
        if (straightResult.failed() || straightResult.result == null || straightResult.result.isEmpty()) return null

        val waypointItem: StraightPathItem = if (straightResult.result.size >= 2) {
            straightResult.result[1]
        } else {
            straightResult.result[0]
        }
        val p = waypointItem.pos
        return AnimalWaypoint(
            x = p[0].toDouble(),
            y = p[1].toDouble(),
            z = p[2].toDouble()
        )
    }

    private fun queryBounds(
        startX: Double,
        startY: Double,
        startZ: Double,
        targetX: Double,
        targetY: Double,
        targetZ: Double
    ): QueryBounds {
        val minX = floor(min(startX, targetX) - NAV_MARGIN_BLOCKS).toInt()
        val maxX = ceil(max(startX, targetX) + NAV_MARGIN_BLOCKS).toInt()
        val minZ = floor(min(startZ, targetZ) - NAV_MARGIN_BLOCKS).toInt()
        val maxZ = ceil(max(startZ, targetZ) + NAV_MARGIN_BLOCKS).toInt()
        val minY = floor(min(startY, targetY) - NAV_VERTICAL_MARGIN).toInt()
        val maxY = ceil(max(startY, targetY) + NAV_VERTICAL_MARGIN).toInt()
        return QueryBounds(
            minX = minX,
            maxX = maxX,
            minY = minY,
            maxY = maxY,
            minZ = minZ,
            maxZ = maxZ
        )
    }

    private fun buildSurfaceGeometry(
        minX: Int,
        maxX: Int,
        minY: Int,
        maxY: Int,
        minZ: Int,
        maxZ: Int,
        requiredHeight: Double
    ): SurfaceGeometry {
        val vertices = ArrayList<Float>()
        val faces = ArrayList<Int>()
        var nextVertex = 0

        for (x in minX until maxX) {
            for (z in minZ until maxZ) {
                val y = findWalkableFloorY(x, z, minY, maxY, requiredHeight) ?: continue
                val fy = y.toFloat()

                val v0 = floatArrayOf(x.toFloat(), fy, z.toFloat())
                val v1 = floatArrayOf(x.toFloat(), fy, (z + 1).toFloat())
                val v2 = floatArrayOf((x + 1).toFloat(), fy, (z + 1).toFloat())
                val v3 = floatArrayOf((x + 1).toFloat(), fy, z.toFloat())

                addVertex(vertices, v0)
                addVertex(vertices, v1)
                addVertex(vertices, v2)
                addVertex(vertices, v3)

                // Counter-clockwise winding viewed from above so normals point upward.
                faces.add(nextVertex)
                faces.add(nextVertex + 1)
                faces.add(nextVertex + 2)
                faces.add(nextVertex)
                faces.add(nextVertex + 2)
                faces.add(nextVertex + 3)
                nextVertex += 4
            }
        }

        return SurfaceGeometry(
            vertices = vertices.toFloatArray(),
            faces = faces.toIntArray()
        )
    }

    private fun addVertex(out: MutableList<Float>, vertex: FloatArray) {
        out.add(vertex[0])
        out.add(vertex[1])
        out.add(vertex[2])
    }

    private fun findWalkableFloorY(
        x: Int,
        z: Int,
        minY: Int,
        maxY: Int,
        requiredHeight: Double
    ): Int? {
        val clearance = max(1, ceil(requiredHeight).toInt())
        for (y in maxY downTo minY) {
            val supportState = blockStateAt(x, y - 1, z)
            if (!isSolidState(supportState)) continue

            var blocked = false
            for (offset in 0 until clearance) {
                val state = blockStateAt(x, y + offset, z)
                if (isSolidState(state)) {
                    blocked = true
                    break
                }
            }
            if (blocked) continue

            return y
        }
        return null
    }

    private data class SurfaceGeometry(
        val vertices: FloatArray,
        val faces: IntArray
    )

    private data class QueryBounds(
        val minX: Int,
        val maxX: Int,
        val minY: Int,
        val maxY: Int,
        val minZ: Int,
        val maxZ: Int
    )

    companion object {
        private const val NAV_WALK_FLAG = 1

        private const val CELL_SIZE = 0.5f
        private const val CELL_HEIGHT = 0.2f

        private const val MIN_AGENT_HEIGHT = 0.8f
        private const val MIN_AGENT_RADIUS = 0.2f
        private const val MIN_AGENT_CLIMB = 0.3f
        private const val MAX_AGENT_SLOPE = 45.0f

        private const val REGION_MIN_SIZE = 4
        private const val REGION_MERGE_SIZE = 12
        private const val EDGE_MAX_LEN = 12.0f
        private const val EDGE_MAX_ERROR = 1.3f
        private const val MAX_VERTS_PER_POLY = 6
        private const val DETAIL_SAMPLE_DIST = 6.0f
        private const val DETAIL_SAMPLE_MAX_ERROR = 1.0f

        private const val NAV_MARGIN_BLOCKS = 3.0
        private const val NAV_VERTICAL_MARGIN = 5.0

        private const val SEARCH_HALF_EXTENT_XZ = 2.0f
        private const val SEARCH_HALF_EXTENT_Y = 4.0f
        private const val MAX_STRAIGHT_PATH_POINTS = 32
    }
}
