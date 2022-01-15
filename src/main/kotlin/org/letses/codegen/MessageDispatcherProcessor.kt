/*
 * Copyright (c) 2022 LETSES.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.letses.codegen

import com.google.auto.service.AutoService
import com.squareup.kotlinpoet.*
import kotlinx.collections.immutable.ImmutableList
import kotlinx.collections.immutable.ImmutableMap
import kotlinx.collections.immutable.ImmutableSet
import java.io.File
import javax.annotation.processing.AbstractProcessor
import javax.annotation.processing.Processor
import javax.annotation.processing.RoundEnvironment
import javax.lang.model.SourceVersion
import javax.lang.model.element.Element
import javax.lang.model.element.ExecutableElement
import javax.lang.model.element.TypeElement
import javax.lang.model.type.DeclaredType
import javax.lang.model.type.MirroredTypeException
import javax.lang.model.type.TypeMirror
import javax.tools.Diagnostic
import kotlin.reflect.KClass

@AutoService(Processor::class)
class MessageDispatcherProcessor : AbstractProcessor() {

    companion object {
        const val KAPT_KOTLIN_GENERATED_OPTION_NAME = "kapt.kotlin.generated"
    }

    override fun getSupportedAnnotationTypes(): MutableSet<String> {
        return mutableSetOf(org.letses.codegen.MessageHandler::class.java.name)
    }

    override fun getSupportedSourceVersion(): SourceVersion {
        return SourceVersion.latest()
    }

    override fun getSupportedOptions(): MutableSet<String> {
        return mutableSetOf(org.letses.codegen.MessageDispatcherProcessor.Companion.KAPT_KOTLIN_GENERATED_OPTION_NAME)
    }

    override fun process(annotations: MutableSet<out TypeElement>, roundEnv: RoundEnvironment): Boolean {
        val handlerFunctions = roundEnv.getElementsAnnotatedWith(org.letses.codegen.MessageHandler::class.java)
        val grouped = handlerFunctions.groupBy { it.pkg to it.simpleName.toString() }
        grouped.forEach { pkgAndName, fnList ->
            process(pkgAndName, fnList)
        }
        return true
    }

    private fun process(pkgAndName: Pair<String, String>, rawList: List<Element>) {
        val (pkg, name) = pkgAndName
        processingEnv.messager.printMessage(Diagnostic.Kind.NOTE, "Generating dispatcher for $pkgAndName")
        val fnList = rawList.map { it as ExecutableElement }
        val parameterListSize = fnList[0].parameters.size
        require(fnList.all { it.parameters.size == parameterListSize })

        val varPosition = fnList.find { it.hasAnnotation(org.letses.codegen.MessageHandlerVarParam::class) }
            ?.getAnnotation(org.letses.codegen.MessageHandlerVarParam::class.java)?.position
            ?: (parameterListSize - 1)

        val baseType = fnList.find { it.hasAnnotation(org.letses.codegen.MessageHandlerBaseClass::class) }?.let {
            val t = try {
                it.getAnnotation(org.letses.codegen.MessageHandlerBaseClass::class.java).baseCls
                throw AssertionError()
            } catch (mte: MirroredTypeException) {
                mte.typeMirror
            }
            t.kotlinType
        } ?: Any::class.asTypeName()

        val withDefaultBranch =
            fnList.find { it.hasAnnotation(org.letses.codegen.MessageHandlerWithDefaultBranch::class) } != null

        val internalModifier =
            fnList.find { it.hasAnnotation(org.letses.codegen.MessageHandlerInternalDispatchFunction::class) } != null

        val receiver =
            fnList.find { it.hasAnnotation(org.letses.codegen.MessageHandlerWithReceiver::class) }?.enclosingElement

        val parameters = fnList[0].parameters.map { it.simpleName.toString() to it.asType().kotlinType }
        val varTypes = fnList.map {
            val t = it.parameters[varPosition].asType()
            val applyArgsBlock = it.parameters.mapIndexed { i, p ->
                val n = p.simpleName.toString()
                if (i == varPosition) "$n = %L"
                else "$n = $n"
            }.joinToString(", ")
            t to applyArgsBlock
        }

        val fnBldr = FunSpec.builder(name + "Dispatch")
        receiver?.let {
            fnBldr.receiver(it.asType().kotlinType)
        }
        parameters.forEachIndexed { i, (n, t) ->
            val t2 = if (i == varPosition) baseType else t
            fnBldr.addParameter(n, t2)
        }
        fnBldr.beginControlFlow("return when(%L)", parameters[varPosition].first)
        varTypes.forEach { (t, applyArgsBlock) ->
            fnBldr.beginControlFlow("is %T ->", t.kotlinType)
            fnBldr.addStatement("$name($applyArgsBlock)", parameters[varPosition].first)
            fnBldr.endControlFlow()
        }

        if (withDefaultBranch) {
            fnBldr.beginControlFlow("else ->")
            fnBldr.addStatement("throw %T(%S)", IllegalArgumentException::class.asTypeName(), "unsupported type")
            fnBldr.endControlFlow()
        }

        fnBldr.endControlFlow()

        if (internalModifier) {
            fnBldr.addModifiers(KModifier.INTERNAL)
        }

        val prefixName = receiver?.simpleName?.toString() ?: ""
        val fileBldr = FileSpec.builder(pkg, prefixName + name.capitalize() + "Dispatcher")
        fileBldr.addFunction(fnBldr.build())
        fileBldr.build().save()
    }

    private fun FileSpec.save() {
        this.writeTo(File(processingEnv.options[org.letses.codegen.MessageDispatcherProcessor.Companion.KAPT_KOTLIN_GENERATED_OPTION_NAME]))
    }

    private val Element.pkg get() = processingEnv.elementUtils.getPackageOf(this).toString()

    private val TypeMirror.fullName get() = processingEnv.typeUtils.erasure(this).toString()

    private fun Element.hasAnnotation(cls: KClass<out Annotation>): Boolean = this.getAnnotation(cls.java) != null

    private val TypeMirror.kotlinType: TypeName
        get() = when (this.fullName) {
            "byte" -> BYTE
            "java.lang.Byte" -> BYTE
            "char" -> CHAR
            "java.lang.Character" -> CHAR
            "short" -> SHORT
            "java.lang.Short" -> SHORT
            "int" -> INT
            "java.lang.Integer" -> INT
            "long" -> LONG
            "java.lang.Long" -> LONG
            "float" -> FLOAT
            "java.lang.Float" -> FLOAT
            "double" -> DOUBLE
            "java.lang.Double" -> DOUBLE
            "boolean" -> BOOLEAN
            "java.lang.Boolean" -> BOOLEAN
            "java.lang.String" -> String::class.asTypeName()
            "java.lang.Byte[]" -> ClassName.bestGuess("Array<Byte>")
            "java.util.List" -> {
                val itemType = (this as DeclaredType).typeArguments[0]!!
                ParameterizedTypeName.get(List::class.asClassName(), itemType.kotlinType)
            }
            "java.util.Set" -> {
                val itemType = (this as DeclaredType).typeArguments[0]!!
                ParameterizedTypeName.get(Set::class.asClassName(), itemType.kotlinType)
            }
            "java.util.Map" -> {
                val keyType = (this as DeclaredType).typeArguments[0]!!
                val valueType = this.typeArguments[1]!!
                ParameterizedTypeName.get(
                    Map::class.asClassName(),
                    keyType.kotlinType, valueType.kotlinType
                )
            }
            "kotlinx.collections.immutable.ImmutableList" -> {
                val itemType = (this as DeclaredType).typeArguments[0]!!
                ParameterizedTypeName.get(ImmutableList::class.asClassName(), itemType.kotlinType)
            }
            "kotlinx.collections.immutable.ImmutableSet" -> {
                val itemType = (this as DeclaredType).typeArguments[0]!!
                ParameterizedTypeName.get(ImmutableSet::class.asClassName(), itemType.kotlinType)
            }
            "kotlinx.collections.immutable.ImmutableMap" -> {
                val keyType = (this as DeclaredType).typeArguments[0]!!
                val valueType = this.typeArguments[1]!!
                ParameterizedTypeName.get(
                    ImmutableMap::class.asClassName(),
                    keyType.kotlinType, valueType.kotlinType
                )
            }
            else -> this.asTypeName()
        }

}
