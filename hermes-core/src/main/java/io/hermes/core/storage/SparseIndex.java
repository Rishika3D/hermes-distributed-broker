package io.hermes.core.storage;

import java.util.ArrayList;
import java.util.List;

/**
 * Sparse offset index for one log segment: maps every Nth record's offset to its
 * byte position in the segment file, so reads seek close to the target instead of
 * scanning from the start.
 */
public final class SparseIndex {

    private final List<long[]> entries = new ArrayList<>();

    public synchronized void add(long offset, long position) {
        entries.add(new long[]{offset, position});
    }

    /**
     * Byte position of the greatest indexed offset that is {@code <= offset},
     * or 0 if nothing indexed below it (scan from the start of the file).
     */
    public synchronized long floorPosition(long offset) {
        long position = 0;
        int low = 0;
        int high = entries.size() - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (entries.get(mid)[0] <= offset) {
                position = entries.get(mid)[1];
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return position;
    }

    public synchronized int size() {
        return entries.size();
    }
}
