package com.github.boneill42;

import com.netflix.astyanax.annotations.Component;

public class FishBlogColumn {
    public class Key {
        @Component(ordinal = 0)
        public long when;
        @Component(ordinal = 1)
        public String fishtype;
        @Component(ordinal = 2)
        public String field;

        public Key() {
        }
    }
    public Key key = new Key();

    public byte[] value;

    public FishBlogColumn() {
    }
}