package org.macaroon3145.api.mixin

import kotlin.reflect.KClass

enum class MixinAt {
    HEAD,
    MIDDLE,
    TAIL,
    LINE
}

enum class FieldAccess {
    GET,
    PUT
}

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class Mixin(
    val target: KClass<*> = Any::class
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Inject(
    val method: String,
    val target: KClass<*> = Any::class,
    val descriptor: String = "",
    val at: MixinAt = MixinAt.HEAD,
    val line: Int = -1,
    val cancellable: Boolean = false,
    val order: Int = 0
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Wrap(
    val method: String,
    val target: KClass<*> = Any::class,
    val descriptor: String = "",
    val before: Boolean = true,
    val after: Boolean = true,
    val cancellable: Boolean = false,
    val order: Int = 0
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Overwrite(
    val method: String,
    val target: KClass<*> = Any::class,
    val descriptor: String = "",
    val order: Int = 0
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class ModifyField(
    val method: String,
    val target: KClass<*> = Any::class,
    val descriptor: String = "",
    val fieldName: String,
    val fieldDescriptor: String,
    val access: FieldAccess = FieldAccess.GET,
    val order: Int = 0
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class ModifyIntConstant(
    val method: String,
    val target: KClass<*> = Any::class,
    val descriptor: String = "",
    val value: Int,
    val order: Int = 0
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class ModifyStringConstant(
    val method: String,
    val target: KClass<*> = Any::class,
    val descriptor: String = "",
    val value: String,
    val order: Int = 0
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class RedirectNew(
    val method: String,
    val target: KClass<*> = Any::class,
    val descriptor: String = "",
    val constructorOwnerClassName: String,
    val constructorDescriptor: String,
    val order: Int = 0
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class RedirectCall(
    val method: String,
    val target: KClass<*> = Any::class,
    val descriptor: String = "",
    val ownerClassName: String,
    val name: String,
    val targetDescriptor: String = "",
    val order: Int = 0
)

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class ModifyReturn(
    val method: String,
    val target: KClass<*> = Any::class,
    val descriptor: String = "",
    val order: Int = 0
)
