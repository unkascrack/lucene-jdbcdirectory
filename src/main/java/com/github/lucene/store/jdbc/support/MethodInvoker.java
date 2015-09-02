/*
 * Copyright 2004-2009 the original author or authors.
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

package com.github.lucene.store.jdbc.support;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Helper class that allows to specify a method to invoke in a declarative
 * fashion, be it static or non-static.
 *
 * <p>
 * Usage: Specify targetClass/targetMethod respectively
 * targetObject/targetMethod, optionally specify arguments, prepare the invoker.
 * Afterwards, you can invoke the method any number of times.
 *
 * <p>
 * Typically not used directly but via its subclasses MethodInvokingFactoryBean
 * and MethodInvokingJobDetailFactoryBean.
 *
 * @author kimchy
 * @see #prepare
 * @see #invoke
 */
public class MethodInvoker {

    private Class<?> targetClass;

    private Object targetObject;

    private String targetMethod;

    private Object[] arguments;

    // the method we will call
    private Method methodObject;

    // booleans marker if the method is static
    private boolean isStatic;

    /**
     * Set the target class on which to call the target method. Only necessary
     * when the target method is static; else, a target object needs to be
     * specified anyway.
     *
     * @see #setTargetObject
     * @see #setTargetMethod
     */
    public void setTargetClass(final Class<?> targetClass) {
        this.targetClass = targetClass;
    }

    /**
     * Return the target class on which to call the target method.
     */
    public Class<?> getTargetClass() {
        return targetClass;
    }

    /**
     * Set the target object on which to call the target method. Only necessary
     * when the target method is not static; else, a target class is sufficient.
     *
     * @see #setTargetClass
     * @see #setTargetMethod
     */
    public void setTargetObject(final Object targetObject) {
        this.targetObject = targetObject;
        if (targetObject != null) {
            targetClass = targetObject.getClass();
        }
    }

    /**
     * Return the target object on which to call the target method.
     */
    public Object getTargetObject() {
        return targetObject;
    }

    /**
     * Set the name of the method to be invoked. Refers to either a static
     * method or a non-static method, depending on a target object being set.
     *
     * @see #setTargetClass
     * @see #setTargetObject
     */
    public void setTargetMethod(final String targetMethod) {
        this.targetMethod = targetMethod;
    }

    /**
     * Return the name of the method to be invoked.
     */
    public String getTargetMethod() {
        return targetMethod;
    }

    /**
     * Set a fully qualified static method name to invoke, e.g.
     * "example.MyExampleClass.myExampleMethod". Convenient alternative to
     * specifying targetClass and targetMethod.
     *
     * @see #setTargetClass
     * @see #setTargetMethod
     */
    public void setStaticMethod(final String staticMethod) throws ClassNotFoundException {
        final int lastDotIndex = staticMethod.lastIndexOf('.');
        if (lastDotIndex == -1 || lastDotIndex == staticMethod.length()) {
            throw new IllegalArgumentException("staticMethod must be a fully qualified class plus method name: "
                    + "e.g. 'example.MyExampleClass.myExampleMethod'");
        }
        final String className = staticMethod.substring(0, lastDotIndex);
        final String methodName = staticMethod.substring(lastDotIndex + 1);
        setTargetClass(Class.forName(className, true, Thread.currentThread().getContextClassLoader()));
        setTargetMethod(methodName);
    }

    /**
     * Set arguments for the method invocation. If this property is not set, or
     * the Object array is of length 0, a method with no arguments is assumed.
     */
    public void setArguments(final Object[] arguments) {
        this.arguments = arguments;
    }

    /**
     * Retrun the arguments for the method invocation.
     */
    public Object[] getArguments() {
        return arguments;
    }

    /**
     * Prepare the specified method. The method can be invoked any number of
     * times afterwards.
     *
     * @see #getPreparedMethod
     * @see #invoke
     */
    public MethodInvoker prepare() throws ClassNotFoundException, NoSuchMethodException {
        if (targetClass == null) {
            throw new IllegalArgumentException("Either targetClass or targetObject is required");
        }
        if (targetMethod == null) {
            throw new IllegalArgumentException("targetMethod is required");
        }

        if (arguments == null) {
            arguments = new Object[0];
        }

        final Class<?>[] argTypes = new Class[arguments.length];
        for (int i = 0; i < arguments.length; ++i) {
            argTypes[i] = arguments[i] != null ? arguments[i].getClass() : Object.class;
        }

        // Try to get the exact method first.
        try {
            methodObject = targetClass.getMethod(targetMethod, argTypes);
        } catch (final NoSuchMethodException ex) {
            // Just rethrow exception if we can't get any match.
            methodObject = findMatchingMethod();
            if (methodObject == null) {
                throw ex;
            }
        }

        isStatic = Modifier.isStatic(methodObject.getModifiers());

        return this;
    }

    /**
     * Find a matching method with the specified name for the specified
     * arguments.
     *
     * @return a matching method, or <code>null</code> if none
     * @see #getTargetClass()
     * @see #getTargetMethod()
     * @see #getArguments()
     */
    protected Method findMatchingMethod() {
        final Method[] candidates = getTargetClass().getMethods();
        final int argCount = getArguments().length;
        Method matchingMethod = null;
        int numberOfMatchingMethods = 0;

        for (final Method candidate : candidates) {
            // Check if the inspected method has the correct name and number of
            // parameters.
            if (candidate.getName().equals(getTargetMethod()) && candidate.getParameterTypes().length == argCount) {
                matchingMethod = candidate;
                numberOfMatchingMethods++;
            }
        }

        // Only return matching method if exactly one found.
        if (numberOfMatchingMethods == 1) {
            return matchingMethod;
        } else {
            return null;
        }
    }

    /**
     * Return the prepared Method object that will be invoker. Can for example
     * be used to determine the return type.
     *
     * @see #prepare
     * @see #invoke
     */
    public Method getPreparedMethod() {
        return methodObject;
    }

    /**
     * Invoke the specified method. The invoker needs to have been prepared
     * before.
     *
     * @return the object (possibly null) returned by the method invocation, or
     *         <code>null</code> if the method has a void return type
     * @see #prepare
     */
    public Object invoke() throws InvocationTargetException, IllegalAccessException {
        if (methodObject == null) {
            throw new IllegalStateException("prepare() must be called prior to invoke() on MethodInvoker");
        }
        // In the static case, target will just be <code>null</code>.
        return methodObject.invoke(targetObject, arguments);
    }

    /**
     * Another invoke option. Here, the target object and the arguments can be
     * specified after the prerpare method has been called. Note, that the
     * arguments should be provided before the prepare as well so the method can
     * be found (or a template of the arguments).
     */
    public Object invoke(final Object targetObject, Object[] arguments)
            throws InvocationTargetException, IllegalAccessException {
        if (methodObject == null) {
            throw new IllegalStateException("prepare() must be called prior to invoke() on MethodInvoker");
        }
        if (targetObject == null && !isStatic) {
            throw new IllegalArgumentException("Target method must not be non-static without a target");
        }
        if (arguments == null) {
            arguments = new Object[0];
        }
        // In the static case, target will just be <code>null</code>.
        return methodObject.invoke(targetObject, arguments);
    }
}
