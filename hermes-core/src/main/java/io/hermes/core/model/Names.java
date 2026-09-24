package io.hermes.core.model;

import java.util.regex.Pattern;

/**
 * Validation for user-supplied identifiers (topic and group names). The wire
 * protocol encodes structured data with {@code , ; :} separators and names
 * appear in REST paths and directory names, so the allowed alphabet is
 * restricted to make injection into any of those layers impossible.
 */
public final class Names {

    public static final int MAX_LENGTH = 249;

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9._-]+");

    private Names() {
    }

    public static String requireValid(String name, String what) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException(what + " is required");
        }
        if (name.length() > MAX_LENGTH) {
            throw new IllegalArgumentException(what + " exceeds " + MAX_LENGTH + " characters");
        }
        if (name.equals(".") || name.equals("..")) {
            throw new IllegalArgumentException(what + " must not be a path reference");
        }
        if (!VALID.matcher(name).matches()) {
            throw new IllegalArgumentException(
                    what + " may only contain letters, digits, '.', '_' and '-': " + name);
        }
        return name;
    }
}
