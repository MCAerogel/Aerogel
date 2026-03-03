package org.macaroon3145.plugin.mixin

import net.bytebuddy.agent.ByteBuddyAgent
import org.macaroon3145.api.mixin.FieldAccess
import org.macaroon3145.api.mixin.Inject
import org.macaroon3145.api.mixin.Mixin
import org.macaroon3145.api.mixin.MixinAt
import org.macaroon3145.api.mixin.ModifyField
import org.macaroon3145.api.mixin.ModifyIntConstant
import org.macaroon3145.api.mixin.ModifyReturn
import org.macaroon3145.api.mixin.ModifyStringConstant
import org.macaroon3145.api.mixin.Overwrite
import org.macaroon3145.api.mixin.RedirectCall
import org.macaroon3145.api.mixin.RedirectNew
import org.macaroon3145.api.mixin.Wrap
import org.objectweb.asm.ClassReader
import org.objectweb.asm.ClassVisitor
import org.objectweb.asm.ClassWriter
import org.objectweb.asm.Label
import org.objectweb.asm.MethodVisitor
import org.objectweb.asm.Opcodes
import org.objectweb.asm.Type
import org.objectweb.asm.commons.AdviceAdapter
import org.objectweb.asm.commons.Method as AsmMethod
import org.slf4j.LoggerFactory
import java.lang.instrument.ClassFileTransformer
import java.lang.instrument.Instrumentation
import java.lang.reflect.Method as ReflectMethod
import java.lang.reflect.Modifier
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

internal object MixinWeavingEngine {
    private val logger = LoggerFactory.getLogger(MixinWeavingEngine::class.java)
    private val started = AtomicBoolean(false)
    private val specsByTarget = ConcurrentHashMap<String, CopyOnWriteArrayList<HookSpec>>()
    private val appliedHookKeysByClass = ConcurrentHashMap<String, MutableSet<String>>()

    private val instrumentation: Instrumentation by lazy { ByteBuddyAgent.install() }

    private val transformer = object : ClassFileTransformer {
        override fun transform(
            loader: ClassLoader?,
            className: String?,
            classBeingRedefined: Class<*>?,
            protectionDomain: java.security.ProtectionDomain?,
            classfileBuffer: ByteArray?
        ): ByteArray? {
            if (className == null || classfileBuffer == null) return null
            return transformClass(className, classfileBuffer)
        }
    }

    fun registerPluginMixins(owner: String, classLoader: ClassLoader, pluginJar: Path) {
        ensureStarted()
        val specs = readAnnotationBasedSpecs(owner, classLoader, pluginJar)
        if (specs.isEmpty()) return

        for (spec in specs) {
            specsByTarget.computeIfAbsent(spec.targetInternalName) { CopyOnWriteArrayList() }.add(spec)
        }

        val loadedClasses = instrumentation.allLoadedClasses.associateBy { it.name }
        val targetClasses = specs.map { it.targetClassName }.toSet()
        for (targetClass in targetClasses) {
            val clazz = loadedClasses[targetClass] ?: continue
            if (!instrumentation.isModifiableClass(clazz)) continue
            runCatching { instrumentation.retransformClasses(clazz) }
                .onFailure { logger.warn("Failed to retransform mixin target class {}", targetClass, it) }
        }

        logger.info("Registered {} mixin hook(s) for plugin {}", specs.size, owner)
    }

    fun unregisterOwner(owner: String) {
        MixinCallbackBridge.unregisterOwner(owner)
        val it = specsByTarget.entries.iterator()
        while (it.hasNext()) {
            val entry = it.next()
            entry.value.removeIf { spec -> spec.owner == owner }
            if (entry.value.isEmpty()) it.remove()
        }
    }

    private fun ensureStarted() {
        if (!started.compareAndSet(false, true)) return
        instrumentation.addTransformer(transformer, true)
    }

    private fun readAnnotationBasedSpecs(
        owner: String,
        classLoader: ClassLoader,
        pluginJar: Path
    ): List<HookSpec> {
        val out = ArrayList<HookSpec>()
        val classNames = discoverClassNames(pluginJar)
        for (className in classNames) {
            val klass = runCatching { Class.forName(className, false, classLoader) }.getOrNull() ?: continue
            val mixin = klass.getAnnotation(Mixin::class.java) ?: continue
            val targetClassName = mixin.target.trim()
            if (targetClassName.isEmpty()) {
                logger.warn("Skipping @Mixin {} because target is empty.", className)
                continue
            }

            val methods = runCatching { klass.declaredMethods.toList() }.getOrDefault(emptyList())
            for ((index, method) in methods.withIndex()) {
                if (!Modifier.isStatic(method.modifiers)) {
                    if (hasAnyMixinMethodAnnotation(method)) {
                        logger.warn(
                            "Skipping mixin method {}#{} because it is not static. Add @JvmStatic.",
                            className,
                            method.name
                        )
                    }
                    continue
                }

                method.getAnnotation(Inject::class.java)?.let { ann ->
                    val hookKey = "$owner:inject:$className#${method.name}:$index"
                    val callback = resolveCallback(owner, hookKey, method) ?: return@let
                    MixinCallbackBridge.register(owner, hookKey, callback)
                    out += HookSpec(
                        kind = HookKind.INJECT,
                        owner = owner,
                        hookKey = hookKey,
                        targetClassName = targetClassName,
                        targetInternalName = targetClassName.replace('.', '/'),
                        methodName = ann.method,
                        methodDescriptor = ann.descriptor,
                        at = ann.at,
                        line = ann.line,
                        cancellable = ann.cancellable,
                        order = ann.order
                    )
                }

                method.getAnnotation(Wrap::class.java)?.let { ann ->
                    val hookKey = "$owner:wrap:$className#${method.name}:$index"
                    val callback = resolveCallback(owner, hookKey, method) ?: return@let
                    MixinCallbackBridge.register(owner, hookKey, callback)
                    out += HookSpec(
                        kind = HookKind.WRAP,
                        owner = owner,
                        hookKey = hookKey,
                        targetClassName = targetClassName,
                        targetInternalName = targetClassName.replace('.', '/'),
                        methodName = ann.method,
                        methodDescriptor = ann.descriptor,
                        wrapBefore = ann.before,
                        wrapAfter = ann.after,
                        cancellable = ann.cancellable,
                        order = ann.order
                    )
                }

                method.getAnnotation(Overwrite::class.java)?.let { ann ->
                    val hookKey = "$owner:overwrite:$className#${method.name}:$index"
                    val callback = resolveCallback(owner, hookKey, method) ?: return@let
                    MixinCallbackBridge.register(owner, hookKey, callback)
                    out += HookSpec(
                        kind = HookKind.OVERWRITE,
                        owner = owner,
                        hookKey = hookKey,
                        targetClassName = targetClassName,
                        targetInternalName = targetClassName.replace('.', '/'),
                        methodName = ann.method,
                        methodDescriptor = ann.descriptor,
                        order = ann.order
                    )
                }

                method.getAnnotation(ModifyField::class.java)?.let { ann ->
                    val hookKey = "$owner:field:$className#${method.name}:$index"
                    val callback = resolveCallback(owner, hookKey, method) ?: return@let
                    MixinCallbackBridge.register(owner, hookKey, callback)
                    out += HookSpec(
                        kind = HookKind.MODIFY_FIELD,
                        owner = owner,
                        hookKey = hookKey,
                        targetClassName = targetClassName,
                        targetInternalName = targetClassName.replace('.', '/'),
                        methodName = ann.method,
                        methodDescriptor = ann.descriptor,
                        fieldName = ann.fieldName,
                        fieldDescriptor = ann.fieldDescriptor,
                        fieldAccess = ann.access,
                        order = ann.order
                    )
                }

                method.getAnnotation(ModifyIntConstant::class.java)?.let { ann ->
                    val hookKey = "$owner:const-int:$className#${method.name}:$index"
                    val callback = resolveCallback(owner, hookKey, method) ?: return@let
                    MixinCallbackBridge.register(owner, hookKey, callback)
                    out += HookSpec(
                        kind = HookKind.MODIFY_INT_CONSTANT,
                        owner = owner,
                        hookKey = hookKey,
                        targetClassName = targetClassName,
                        targetInternalName = targetClassName.replace('.', '/'),
                        methodName = ann.method,
                        methodDescriptor = ann.descriptor,
                        intConstant = ann.value,
                        order = ann.order
                    )
                }

                method.getAnnotation(ModifyStringConstant::class.java)?.let { ann ->
                    val hookKey = "$owner:const-str:$className#${method.name}:$index"
                    val callback = resolveCallback(owner, hookKey, method) ?: return@let
                    MixinCallbackBridge.register(owner, hookKey, callback)
                    out += HookSpec(
                        kind = HookKind.MODIFY_STRING_CONSTANT,
                        owner = owner,
                        hookKey = hookKey,
                        targetClassName = targetClassName,
                        targetInternalName = targetClassName.replace('.', '/'),
                        methodName = ann.method,
                        methodDescriptor = ann.descriptor,
                        stringConstant = ann.value,
                        order = ann.order
                    )
                }

                method.getAnnotation(RedirectNew::class.java)?.let { ann ->
                    val hookKey = "$owner:redirect-new:$className#${method.name}:$index"
                    val callback = resolveCallback(owner, hookKey, method) ?: return@let
                    MixinCallbackBridge.register(owner, hookKey, callback)
                    out += HookSpec(
                        kind = HookKind.REDIRECT_NEW,
                        owner = owner,
                        hookKey = hookKey,
                        targetClassName = targetClassName,
                        targetInternalName = targetClassName.replace('.', '/'),
                        methodName = ann.method,
                        methodDescriptor = ann.descriptor,
                        constructorOwnerInternalName = ann.constructorOwnerClassName.replace('.', '/'),
                        constructorDescriptor = ann.constructorDescriptor,
                        order = ann.order
                    )
                }

                method.getAnnotation(RedirectCall::class.java)?.let { ann ->
                    val hookKey = "$owner:redirect-call:$className#${method.name}:$index"
                    val callback = resolveCallback(owner, hookKey, method) ?: return@let
                    MixinCallbackBridge.register(owner, hookKey, callback)
                    out += HookSpec(
                        kind = HookKind.REDIRECT_CALL,
                        owner = owner,
                        hookKey = hookKey,
                        targetClassName = targetClassName,
                        targetInternalName = targetClassName.replace('.', '/'),
                        methodName = ann.method,
                        methodDescriptor = ann.descriptor,
                        callOwnerInternalName = ann.ownerClassName.replace('.', '/'),
                        callName = ann.name,
                        callDescriptor = ann.targetDescriptor,
                        order = ann.order
                    )
                }

                method.getAnnotation(ModifyReturn::class.java)?.let { ann ->
                    val hookKey = "$owner:modify-return:$className#${method.name}:$index"
                    val callback = resolveCallback(owner, hookKey, method) ?: return@let
                    MixinCallbackBridge.register(owner, hookKey, callback)
                    out += HookSpec(
                        kind = HookKind.MODIFY_RETURN,
                        owner = owner,
                        hookKey = hookKey,
                        targetClassName = targetClassName,
                        targetInternalName = targetClassName.replace('.', '/'),
                        methodName = ann.method,
                        methodDescriptor = ann.descriptor,
                        order = ann.order
                    )
                }
            }
        }
        return out
    }

    private fun hasAnyMixinMethodAnnotation(method: ReflectMethod): Boolean {
        return method.isAnnotationPresent(Inject::class.java) ||
            method.isAnnotationPresent(Wrap::class.java) ||
            method.isAnnotationPresent(Overwrite::class.java) ||
            method.isAnnotationPresent(ModifyField::class.java) ||
            method.isAnnotationPresent(ModifyIntConstant::class.java) ||
            method.isAnnotationPresent(ModifyStringConstant::class.java) ||
            method.isAnnotationPresent(RedirectNew::class.java) ||
            method.isAnnotationPresent(RedirectCall::class.java) ||
            method.isAnnotationPresent(ModifyReturn::class.java)
    }

    private fun discoverClassNames(pluginJar: Path): List<String> {
        return runCatching {
            FileSystems.newFileSystem(pluginJar).use { fs ->
                Files.walk(fs.getPath("/")).use { stream ->
                    stream
                        .filter { Files.isRegularFile(it) && it.toString().endsWith(".class") }
                        .map { path ->
                            val normalized = path.toString().removePrefix("/").removeSuffix(".class")
                            normalized.replace('/', '.')
                        }
                        .filter { name -> name.isNotBlank() && !name.contains("module-info") }
                        .toList()
                }
            }
        }.getOrDefault(emptyList())
    }

    private fun resolveCallback(
        owner: String,
        hookKey: String,
        method: ReflectMethod
    ): ((Array<Any?>) -> Any?)? {
        method.isAccessible = true
        return { params ->
            try {
                when (method.parameterCount) {
                    0 -> method.invoke(null)
                    else -> {
                        val args = Array(method.parameterCount) { idx -> params.getOrNull(idx) }
                        method.invoke(null, *args)
                    }
                }
            } catch (error: Throwable) {
                logger.error("Mixin callback failed (owner={}, hook={})", owner, hookKey, error)
                null
            }
        }
    }

    private fun transformClass(classInternalName: String, classfileBuffer: ByteArray): ByteArray? {
        val specs = specsByTarget[classInternalName]?.toList().orEmpty()
        if (specs.isEmpty()) return null

        val appliedSet = appliedHookKeysByClass.computeIfAbsent(classInternalName) { ConcurrentHashMap.newKeySet() }
        val pending = specs.filter { spec -> !appliedSet.contains(spec.hookKey) }
        if (pending.isEmpty()) return null

        var changed = false
        val reader = ClassReader(classfileBuffer)
        val writer = ClassWriter(reader, ClassWriter.COMPUTE_MAXS)
        val visitor = object : ClassVisitor(Opcodes.ASM9, writer) {
            override fun visitMethod(
                access: Int,
                name: String,
                descriptor: String,
                signature: String?,
                exceptions: Array<out String>?
            ): MethodVisitor {
                val base = super.visitMethod(access, name, descriptor, signature, exceptions)
                val methodSpecs = pending
                    .filter { it.methodName == name && (it.methodDescriptor.isBlank() || it.methodDescriptor == descriptor) }
                    .sortedWith(compareBy<HookSpec> { it.order }.thenBy { it.hookKey })
                if (methodSpecs.isEmpty()) return base
                changed = true

                return object : AdviceAdapter(Opcodes.ASM9, base, access, name, descriptor) {
                    private val returnType: Type = Type.getReturnType(descriptor)
                    private val objectType: Type = Type.getObjectType("java/lang/Object")
                    private val middleInjectedHookKeys = HashSet<String>()
                    private var firstExecutableVisited = false

                    private val injectSpecs = methodSpecs.filter { it.kind == HookKind.INJECT }
                    private val wrapSpecs = methodSpecs.filter { it.kind == HookKind.WRAP }
                    private val overwriteSpec = methodSpecs.firstOrNull { it.kind == HookKind.OVERWRITE }
                    private val fieldSpecs = methodSpecs.filter { it.kind == HookKind.MODIFY_FIELD }
                    private val intConstSpecs = methodSpecs.filter { it.kind == HookKind.MODIFY_INT_CONSTANT }
                    private val strConstSpecs = methodSpecs.filter { it.kind == HookKind.MODIFY_STRING_CONSTANT }
                    private val redirectNewSpecs = methodSpecs.filter { it.kind == HookKind.REDIRECT_NEW }
                    private val redirectCallSpecs = methodSpecs.filter { it.kind == HookKind.REDIRECT_CALL }
                    private val modifyReturnSpecs = methodSpecs.filter { it.kind == HookKind.MODIFY_RETURN }

                    override fun onMethodEnter() {
                        if (overwriteSpec != null && name != "<init>" && name != "<clinit>") {
                            emitOverwrite(overwriteSpec)
                            appliedSet.add(overwriteSpec.hookKey)
                            return
                        }

                        for (spec in wrapSpecs) {
                            if (!spec.wrapBefore) continue
                            emitInvokeWithOptionalCancel(spec)
                            appliedSet.add(spec.hookKey)
                        }

                        for (spec in injectSpecs) {
                            if (spec.at != MixinAt.HEAD) continue
                            emitInvokeWithOptionalCancel(spec)
                            appliedSet.add(spec.hookKey)
                        }
                    }

                    override fun visitLineNumber(line: Int, start: Label) {
                        super.visitLineNumber(line, start)
                        for (spec in injectSpecs) {
                            if (spec.at != MixinAt.LINE) continue
                            if (spec.line != line) continue
                            emitInvokeWithOptionalCancel(spec)
                            appliedSet.add(spec.hookKey)
                        }
                    }

                    override fun visitInsn(opcode: Int) {
                        maybeInjectMiddle()

                        val intConstant = opcodeToIntConstant(opcode)
                        if (intConstant != null) {
                            val targets = intConstSpecs.filter { it.intConstant == intConstant }
                            if (targets.isNotEmpty()) {
                                super.visitInsn(opcode)
                                for (target in targets) {
                                    transformStackTopValue(Type.INT_TYPE, target)
                                    appliedSet.add(target.hookKey)
                                }
                                return
                            }
                        }
                        super.visitInsn(opcode)
                    }

                    override fun visitIntInsn(opcode: Int, operand: Int) {
                        maybeInjectMiddle()
                        if (opcode == BIPUSH || opcode == SIPUSH) {
                            val targets = intConstSpecs.filter { it.intConstant == operand }
                            if (targets.isNotEmpty()) {
                                super.visitIntInsn(opcode, operand)
                                for (target in targets) {
                                    transformStackTopValue(Type.INT_TYPE, target)
                                    appliedSet.add(target.hookKey)
                                }
                                return
                            }
                        }
                        super.visitIntInsn(opcode, operand)
                    }

                    override fun visitLdcInsn(value: Any?) {
                        maybeInjectMiddle()
                        when (value) {
                            is Int -> {
                                val targets = intConstSpecs.filter { it.intConstant == value }
                                if (targets.isNotEmpty()) {
                                    super.visitLdcInsn(value)
                                    for (target in targets) {
                                        transformStackTopValue(Type.INT_TYPE, target)
                                        appliedSet.add(target.hookKey)
                                    }
                                    return
                                }
                            }
                            is String -> {
                                val targets = strConstSpecs.filter { it.stringConstant == value }
                                if (targets.isNotEmpty()) {
                                    super.visitLdcInsn(value)
                                    for (target in targets) {
                                        transformStackTopValue(Type.getType(String::class.java), target)
                                        appliedSet.add(target.hookKey)
                                    }
                                    return
                                }
                            }
                        }
                        super.visitLdcInsn(value)
                    }

                    override fun visitFieldInsn(opcode: Int, owner: String, fieldName: String, fieldDesc: String) {
                        maybeInjectMiddle()
                        val matches = fieldSpecs.filter {
                            it.fieldName == fieldName &&
                                it.fieldDescriptor == fieldDesc &&
                                ((opcode == GETFIELD || opcode == GETSTATIC) && it.fieldAccess == FieldAccess.GET ||
                                    (opcode == PUTFIELD || opcode == PUTSTATIC) && it.fieldAccess == FieldAccess.PUT)
                        }
                        if (matches.isEmpty()) {
                            super.visitFieldInsn(opcode, owner, fieldName, fieldDesc)
                            return
                        }

                        val fieldType = Type.getType(fieldDesc)
                        when (opcode) {
                            GETFIELD, GETSTATIC -> {
                                super.visitFieldInsn(opcode, owner, fieldName, fieldDesc)
                                for (match in matches) {
                                    transformStackTopValue(fieldType, match)
                                    appliedSet.add(match.hookKey)
                                }
                            }
                            PUTFIELD -> {
                                val valueLocal = newLocal(fieldType)
                                val ownerLocal = newLocal(Type.getObjectType(owner))
                                storeLocal(valueLocal)
                                storeLocal(ownerLocal)
                                loadLocal(valueLocal)
                                for (match in matches) {
                                    transformStackTopValue(fieldType, match)
                                    appliedSet.add(match.hookKey)
                                }
                                storeLocal(valueLocal)
                                loadLocal(ownerLocal)
                                loadLocal(valueLocal)
                                super.visitFieldInsn(opcode, owner, fieldName, fieldDesc)
                            }
                            PUTSTATIC -> {
                                val valueLocal = newLocal(fieldType)
                                storeLocal(valueLocal)
                                loadLocal(valueLocal)
                                for (match in matches) {
                                    transformStackTopValue(fieldType, match)
                                    appliedSet.add(match.hookKey)
                                }
                                storeLocal(valueLocal)
                                loadLocal(valueLocal)
                                super.visitFieldInsn(opcode, owner, fieldName, fieldDesc)
                            }
                            else -> super.visitFieldInsn(opcode, owner, fieldName, fieldDesc)
                        }
                    }

                    override fun visitMethodInsn(
                        opcode: Int,
                        owner: String,
                        methodName: String,
                        methodDesc: String,
                        isInterface: Boolean
                    ) {
                        maybeInjectMiddle()
                        super.visitMethodInsn(opcode, owner, methodName, methodDesc, isInterface)
                        if (opcode != INVOKESPECIAL || methodName != "<init>") {
                            val callMatches = redirectCallSpecs.filter {
                                it.callOwnerInternalName == owner &&
                                    it.callName == methodName &&
                                    (it.callDescriptor.isBlank() || it.callDescriptor == methodDesc)
                            }
                            if (callMatches.isNotEmpty()) {
                                val returnType = Type.getReturnType(methodDesc)
                                if (returnType.sort != Type.VOID) {
                                    for (match in callMatches) {
                                        transformStackTopValue(returnType, match)
                                        appliedSet.add(match.hookKey)
                                    }
                                }
                            }
                            return
                        }
                        val ctorMatches = redirectNewSpecs.filter {
                            it.constructorOwnerInternalName == owner && it.constructorDescriptor == methodDesc
                        }
                        if (ctorMatches.isEmpty()) return
                        for (match in ctorMatches) {
                            transformStackTopValue(Type.getObjectType(owner), match)
                            appliedSet.add(match.hookKey)
                        }
                    }

                    override fun onMethodExit(opcode: Int) {
                        if (opcode == ATHROW) return

                        for (spec in injectSpecs) {
                            if (spec.at != MixinAt.TAIL) continue
                            emitInvokeWithOptionalCancel(spec)
                            appliedSet.add(spec.hookKey)
                        }

                        for (spec in wrapSpecs) {
                            if (!spec.wrapAfter) continue
                            if (returnType.sort == Type.VOID) {
                                emitInvokeVoid(spec)
                            } else {
                                val retLocal = newLocal(returnType)
                                storeLocal(retLocal)
                                push(spec.hookKey)
                                loadInstanceOrNull()
                                loadLocal(retLocal)
                                box(returnType)
                                loadArgArray()
                                invokeStatic(
                                    Type.getType(MixinCallbackBridge::class.java),
                                    AsmMethod(
                                        "invokeWrapAfter",
                                        "(Ljava/lang/String;Ljava/lang/Object;Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;"
                                    )
                                )
                                unbox(returnType)
                                storeLocal(retLocal)
                                loadLocal(retLocal)
                            }
                            appliedSet.add(spec.hookKey)
                        }

                        if (returnType.sort != Type.VOID) {
                            for (spec in modifyReturnSpecs) {
                                val retLocal = newLocal(returnType)
                                storeLocal(retLocal)
                                loadLocal(retLocal)
                                transformStackTopValue(returnType, spec)
                                storeLocal(retLocal)
                                loadLocal(retLocal)
                                appliedSet.add(spec.hookKey)
                            }
                        }
                    }

                    private fun maybeInjectMiddle() {
                        if (firstExecutableVisited) return
                        firstExecutableVisited = true
                        for (spec in injectSpecs) {
                            if (spec.at != MixinAt.MIDDLE) continue
                            if (!middleInjectedHookKeys.add(spec.hookKey)) continue
                            emitInvokeWithOptionalCancel(spec)
                            appliedSet.add(spec.hookKey)
                        }
                    }

                    private fun emitOverwrite(spec: HookSpec) {
                        push(spec.hookKey)
                        loadInstanceOrNull()
                        loadArgArray()
                        invokeStatic(
                            Type.getType(MixinCallbackBridge::class.java),
                            AsmMethod("invokeOverwrite", "(Ljava/lang/String;Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;")
                        )
                        if (returnType.sort == Type.VOID) {
                            pop()
                            visitInsn(RETURN)
                            return
                        }
                        unbox(returnType)
                        visitInsn(returnType.getOpcode(IRETURN))
                    }

                    private fun emitInvokeVoid(spec: HookSpec) {
                        push(spec.hookKey)
                        loadInstanceOrNull()
                        loadArgArray()
                        invokeStatic(
                            Type.getType(MixinCallbackBridge::class.java),
                            AsmMethod("invokeVoid", "(Ljava/lang/String;Ljava/lang/Object;[Ljava/lang/Object;)V")
                        )
                    }

                    private fun emitInvokeWithOptionalCancel(spec: HookSpec) {
                        if (!spec.cancellable) {
                            emitInvokeVoid(spec)
                            return
                        }
                        push(spec.hookKey)
                        loadInstanceOrNull()
                        loadArgArray()
                        invokeStatic(
                            Type.getType(MixinCallbackBridge::class.java),
                            AsmMethod("invokeBoolean", "(Ljava/lang/String;Ljava/lang/Object;[Ljava/lang/Object;)Z")
                        )
                        val continueLabel = Label()
                        ifZCmp(EQ, continueLabel)
                        emitDefaultReturn()
                        mark(continueLabel)
                    }

                    private fun transformStackTopValue(valueType: Type, spec: HookSpec) {
                        box(valueType)
                        val valueLocal = newLocal(objectType)
                        storeLocal(valueLocal)
                        push(spec.hookKey)
                        loadInstanceOrNull()
                        loadLocal(valueLocal)
                        loadArgArray()
                        invokeStatic(
                            Type.getType(MixinCallbackBridge::class.java),
                            AsmMethod(
                                "invokeValue",
                                "(Ljava/lang/String;Ljava/lang/Object;Ljava/lang/Object;[Ljava/lang/Object;)Ljava/lang/Object;"
                            )
                        )
                        unbox(valueType)
                    }

                    private fun loadInstanceOrNull() {
                        if ((methodAccess and ACC_STATIC) == 0) {
                            loadThis()
                        } else {
                            visitInsn(ACONST_NULL)
                        }
                    }

                    private fun emitDefaultReturn() {
                        when (returnType.sort) {
                            Type.VOID -> visitInsn(RETURN)
                            Type.BOOLEAN,
                            Type.BYTE,
                            Type.CHAR,
                            Type.SHORT,
                            Type.INT -> {
                                visitInsn(ICONST_0)
                                visitInsn(IRETURN)
                            }
                            Type.LONG -> {
                                visitInsn(LCONST_0)
                                visitInsn(LRETURN)
                            }
                            Type.FLOAT -> {
                                visitInsn(FCONST_0)
                                visitInsn(FRETURN)
                            }
                            Type.DOUBLE -> {
                                visitInsn(DCONST_0)
                                visitInsn(DRETURN)
                            }
                            else -> {
                                visitInsn(ACONST_NULL)
                                visitInsn(ARETURN)
                            }
                        }
                    }

                    private fun opcodeToIntConstant(opcode: Int): Int? {
                        return when (opcode) {
                            ICONST_M1 -> -1
                            ICONST_0 -> 0
                            ICONST_1 -> 1
                            ICONST_2 -> 2
                            ICONST_3 -> 3
                            ICONST_4 -> 4
                            ICONST_5 -> 5
                            else -> null
                        }
                    }
                }
            }
        }

        reader.accept(visitor, ClassReader.EXPAND_FRAMES)
        return if (changed) writer.toByteArray() else null
    }

    private enum class HookKind {
        INJECT,
        WRAP,
        OVERWRITE,
        MODIFY_FIELD,
        MODIFY_INT_CONSTANT,
        MODIFY_STRING_CONSTANT,
        REDIRECT_NEW,
        REDIRECT_CALL,
        MODIFY_RETURN
    }

    private data class HookSpec(
        val kind: HookKind,
        val owner: String,
        val hookKey: String,
        val targetClassName: String,
        val targetInternalName: String,
        val methodName: String,
        val methodDescriptor: String,
        val order: Int = 0,
        val at: MixinAt = MixinAt.HEAD,
        val line: Int = -1,
        val cancellable: Boolean = false,
        val wrapBefore: Boolean = true,
        val wrapAfter: Boolean = true,
        val fieldName: String = "",
        val fieldDescriptor: String = "",
        val fieldAccess: FieldAccess = FieldAccess.GET,
        val intConstant: Int = Int.MIN_VALUE,
        val stringConstant: String = "",
        val callOwnerInternalName: String = "",
        val callName: String = "",
        val callDescriptor: String = "",
        val constructorOwnerInternalName: String = "",
        val constructorDescriptor: String = ""
    )
}

internal object MixinCallbackBridge {
    private val callbacks = ConcurrentHashMap<String, (Array<Any?>) -> Any?>()
    private val ownerByHook = ConcurrentHashMap<String, String>()

    fun register(owner: String, hookKey: String, callback: (Array<Any?>) -> Any?) {
        ownerByHook[hookKey] = owner
        callbacks[hookKey] = callback
    }

    fun unregisterOwner(owner: String) {
        val it = ownerByHook.entries.iterator()
        while (it.hasNext()) {
            val entry = it.next()
            if (entry.value == owner) {
                callbacks.remove(entry.key)
                it.remove()
            }
        }
    }

    @JvmStatic
    fun invokeBoolean(hookKey: String, instance: Any?, args: Array<Any?>): Boolean {
        val out = callbacks[hookKey]?.invoke(arrayOf(instance, args))
        return out == true
    }

    @JvmStatic
    fun invokeVoid(hookKey: String, instance: Any?, args: Array<Any?>) {
        callbacks[hookKey]?.invoke(arrayOf(instance, args))
    }

    @JvmStatic
    fun invokeOverwrite(hookKey: String, instance: Any?, args: Array<Any?>): Any? {
        return callbacks[hookKey]?.invoke(arrayOf(instance, args))
    }

    @JvmStatic
    fun invokeValue(hookKey: String, instance: Any?, value: Any?, args: Array<Any?>): Any? {
        return callbacks[hookKey]?.invoke(arrayOf(instance, value, args)) ?: value
    }

    @JvmStatic
    fun invokeWrapAfter(hookKey: String, instance: Any?, value: Any?, args: Array<Any?>): Any? {
        return callbacks[hookKey]?.invoke(arrayOf(instance, value, args)) ?: value
    }
}
