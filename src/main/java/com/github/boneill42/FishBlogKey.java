package com.github.boneill42;

import com.netflix.astyanax.annotations.Component;

/**
 * User: gibbsb
 * Date: 2/19/13
 * Time: 4:46 PM
 */
public class FishBlogKey {
    @Component(ordinal = 0)
    public String userid;
    @Component(ordinal = 1)
    public long when;
    @Component(ordinal = 2)
    public String fishtype;

    public FishBlogKey() {
    }
}
