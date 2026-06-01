package com.personal.kafka.pilot.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaBrokerConfig {

    private String name;
    private String bootstrapServers;

    // SSL/TLS Configuration (all optional)
    private String securityProtocol;  // SSL, SASL_SSL, SASL_PLAINTEXT, PLAINTEXT
    private String sslTruststoreLocation;  // Path to truststore file
    private String sslTruststorePassword;  // Truststore password
    private String sslTruststoreType;      // JKS, PKCS12 (default: JKS)
    private String sslKeystoreLocation;  // Path to keystore file (for mutual TLS)
    private String sslKeystorePassword;    // Keystore password
    private String sslKeyPassword;         // Private key password
    private String sslKeystoreType;        // JKS, PKCS12 (default: JKS)
    private String sslEndpointIdentificationAlgorithm;  // https or empty (default: https)

    @Override
    public String toString() {
        if (bootstrapServers != null && !bootstrapServers.isEmpty()) {
            // Truncate long server lists for display
            String displayServers = bootstrapServers.length() > 30
                    ? bootstrapServers.substring(0, 30) + "..."
                    : bootstrapServers;
            return name + " (" + displayServers + ")";
        }
        return name;
    }
}
