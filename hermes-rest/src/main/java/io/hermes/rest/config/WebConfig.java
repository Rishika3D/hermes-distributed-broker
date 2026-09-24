package io.hermes.rest.config;

import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/** CORS (configurable origins) and the optional API-key filter for /api/**. */
@Configuration
public class WebConfig implements WebMvcConfigurer {

    private final BrokerProperties properties;

    public WebConfig(BrokerProperties properties) {
        this.properties = properties;
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/api/**")
                .allowedOriginPatterns(properties.getCorsOrigins().split(","))
                .allowedHeaders("Content-Type", ApiKeyFilter.HEADER)
                .allowedMethods("GET", "POST", "PUT", "DELETE");
    }

    @Bean
    public FilterRegistrationBean<ApiKeyFilter> apiKeyFilter() {
        FilterRegistrationBean<ApiKeyFilter> registration =
                new FilterRegistrationBean<>(new ApiKeyFilter(properties.getApiKey()));
        registration.addUrlPatterns("/api/*");
        return registration;
    }
}
