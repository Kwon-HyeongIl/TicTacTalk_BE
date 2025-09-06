package com.khi.securityservice.core.exception.type;

import org.springframework.security.core.AuthenticationException;

public class SecurityAuthenticationException extends AuthenticationException {

    public SecurityAuthenticationException(String message) {

        super(message);
    }
}

