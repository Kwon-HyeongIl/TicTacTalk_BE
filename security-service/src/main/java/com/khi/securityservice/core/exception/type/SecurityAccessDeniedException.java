package com.khi.securityservice.core.exception.type;

import java.nio.file.AccessDeniedException;

public class SecurityAccessDeniedException extends AccessDeniedException {

    public SecurityAccessDeniedException(String file, String other, String reason) {

        super(file, other, reason);
    }
}
