package com.tailoredshapes.gridql;

import com.tailoredshapes.stash.Stash;

public interface ApiService {
    void create(Stash payload);
    void update(String id, Stash payload);
    void delete(String id);
}
