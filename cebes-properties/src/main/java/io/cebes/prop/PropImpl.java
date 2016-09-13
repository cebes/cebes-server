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

import java.lang.annotation.Annotation;

class PropImpl implements Prop {

    private final Property value;

    PropImpl(Property value) {
        this.value = value;
    }

    @Override
    public Property value() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Prop)) {
            return false;
        }

        Prop other = (Prop) obj;
        return value.equals(other.value());
    }

    @Override
    public int hashCode() {
        // This is specified in java.lang.Annotation.
        return (127 * "value".hashCode()) ^ value.hashCode();
    }

    @Override
    public String toString() {
        return "@" + Prop.class.getName() + "(value=" + value + ")";
    }

    @Override
    public Class<? extends Annotation> annotationType() {
        return Prop.class;
    }
}
