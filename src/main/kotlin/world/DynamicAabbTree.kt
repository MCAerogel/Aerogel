package org.macaroon3145.world

data class DynamicAabb(
    val minX: Double,
    val minY: Double,
    val minZ: Double,
    val maxX: Double,
    val maxY: Double,
    val maxZ: Double
) {
    fun intersects(other: DynamicAabb): Boolean {
        return maxX > other.minX && minX < other.maxX &&
            maxY > other.minY && minY < other.maxY &&
            maxZ > other.minZ && minZ < other.maxZ
    }

    fun expanded(margin: Double): DynamicAabb {
        return DynamicAabb(
            minX = minX - margin,
            minY = minY - margin,
            minZ = minZ - margin,
            maxX = maxX + margin,
            maxY = maxY + margin,
            maxZ = maxZ + margin
        )
    }

    fun union(other: DynamicAabb): DynamicAabb {
        return DynamicAabb(
            minX = kotlin.math.min(minX, other.minX),
            minY = kotlin.math.min(minY, other.minY),
            minZ = kotlin.math.min(minZ, other.minZ),
            maxX = kotlin.math.max(maxX, other.maxX),
            maxY = kotlin.math.max(maxY, other.maxY),
            maxZ = kotlin.math.max(maxZ, other.maxZ)
        )
    }

    fun surfaceArea(): Double {
        val dx = (maxX - minX).coerceAtLeast(0.0)
        val dy = (maxY - minY).coerceAtLeast(0.0)
        val dz = (maxZ - minZ).coerceAtLeast(0.0)
        return 2.0 * ((dx * dy) + (dy * dz) + (dz * dx))
    }
}

class DynamicAabbTree<T>(
    private val fatMargin: Double = 0.1
) {
    private data class Node<T>(
        var parent: Int = -1,
        var left: Int = -1,
        var right: Int = -1,
        var height: Int = 0,
        var aabb: DynamicAabb = DynamicAabb(0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
        var data: T? = null
    ) {
        val isLeaf: Boolean
            get() = left == -1
    }

    private val nodes = ArrayList<Node<T>>()
    private var root: Int = -1

    fun clear() {
        nodes.clear()
        root = -1
    }

    fun insert(aabb: DynamicAabb, data: T) {
        val leafIndex = nodes.size
        nodes.add(
            Node(
                aabb = aabb.expanded(fatMargin),
                data = data,
                height = 0
            )
        )
        insertLeaf(leafIndex)
    }

    fun query(aabb: DynamicAabb, consumer: (T) -> Unit) {
        if (root == -1) return
        var stack = IntArray(64)
        var size = 0
        stack[size++] = root
        while (size > 0) {
            val nodeIndex = stack[--size]
            val node = nodes[nodeIndex]
            if (!node.aabb.intersects(aabb)) continue
            if (node.isLeaf) {
                val value = node.data ?: continue
                consumer(value)
                continue
            }
            if (size + 2 >= stack.size) {
                stack = stack.copyOf(stack.size * 2)
            }
            stack[size++] = node.left
            stack[size++] = node.right
        }
    }

    private fun insertLeaf(leaf: Int) {
        if (root == -1) {
            root = leaf
            nodes[leaf].parent = -1
            return
        }

        val leafAabb = nodes[leaf].aabb
        var index = root
        while (!nodes[index].isLeaf) {
            val left = nodes[index].left
            val right = nodes[index].right
            val area = nodes[index].aabb.surfaceArea()
            val combinedAabb = nodes[index].aabb.union(leafAabb)
            val combinedArea = combinedAabb.surfaceArea()
            val cost = 2.0 * combinedArea
            val inheritanceCost = 2.0 * (combinedArea - area)

            val costLeft = costForDescent(left, leafAabb, inheritanceCost)
            val costRight = costForDescent(right, leafAabb, inheritanceCost)

            if (cost < costLeft && cost < costRight) {
                break
            }
            index = if (costLeft < costRight) left else right
        }

        val sibling = index
        val oldParent = nodes[sibling].parent
        val newParent = nodes.size
        nodes.add(
            Node(
                parent = oldParent,
                left = sibling,
                right = leaf,
                height = nodes[sibling].height + 1,
                aabb = nodes[sibling].aabb.union(leafAabb)
            )
        )

        nodes[sibling].parent = newParent
        nodes[leaf].parent = newParent

        if (oldParent == -1) {
            root = newParent
        } else {
            if (nodes[oldParent].left == sibling) {
                nodes[oldParent].left = newParent
            } else {
                nodes[oldParent].right = newParent
            }
        }

        var current = nodes[leaf].parent
        while (current != -1) {
            val left = nodes[current].left
            val right = nodes[current].right
            nodes[current].height = 1 + kotlin.math.max(nodes[left].height, nodes[right].height)
            nodes[current].aabb = nodes[left].aabb.union(nodes[right].aabb)
            current = nodes[current].parent
        }
    }

    private fun costForDescent(nodeIndex: Int, leafAabb: DynamicAabb, inheritanceCost: Double): Double {
        val node = nodes[nodeIndex]
        val union = node.aabb.union(leafAabb)
        return if (node.isLeaf) {
            union.surfaceArea() + inheritanceCost
        } else {
            (union.surfaceArea() - node.aabb.surfaceArea()) + inheritanceCost
        }
    }
}
