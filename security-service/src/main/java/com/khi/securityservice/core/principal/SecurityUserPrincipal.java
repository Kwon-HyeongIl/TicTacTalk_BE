package com.khi.securityservice.core.principal;

import com.khi.securityservice.core.entity.security.SecurityUserPrincipalEntity;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.core.user.OAuth2User;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class SecurityUserPrincipal implements OAuth2User {

    private final SecurityUserPrincipalEntity securityUserPrincipalEntity;

    public SecurityUserPrincipal(SecurityUserPrincipalEntity securityUserPrincipalEntity) {

        this.securityUserPrincipalEntity = securityUserPrincipalEntity;
    }

    @Override
    public Map<String, Object> getAttributes() {

        return null;
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {

        Collection<GrantedAuthority> collection = new ArrayList<>();

        collection.add(new GrantedAuthority() {

            @Override
            public String getAuthority() {

                return securityUserPrincipalEntity.getRole();
            }
        });

        return collection;
    }

    @Override
    public String getName() {

        return securityUserPrincipalEntity.getUid();
    }
}