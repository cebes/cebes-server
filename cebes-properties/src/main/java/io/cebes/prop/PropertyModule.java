/* Copyright 2016 The Cebes Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, version 2.0 (the "License").
 * You may not use this work except in compliance with the License,
 * which is available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 *
 * Created by phvu on 09/09/16.
 */

package io.cebes.prop;

import com.google.inject.AbstractModule;

import java.util.stream.Stream;

public class PropertyModule extends AbstractModule {

    private Boolean testPropEnabled = false;

    public PropertyModule(Boolean testPropEnabled) {
        this.testPropEnabled = testPropEnabled;
    }

    @Override
    protected void configure() {
        final IPropertySet propSet = new ConfigPropertySet();
        if (testPropEnabled) {
            Stream.of(Property.values()).forEach(p ->
                    bindConstant().annotatedWith(new PropImpl(p)).to(propSet.get(p)));
        } else {
            Stream.of(Property.values()).filter(p -> !p.isTestProperty()).forEach(p ->
                    bindConstant().annotatedWith(new PropImpl(p)).to(propSet.get(p)));
        }
    }
}
