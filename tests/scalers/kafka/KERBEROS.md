# Kafka Scaler with Kerberos (GSSAPI) Authentication

This guide explains how to configure and use the Kafka scaler with Kerberos authentication in KEDA.

## Overview

The Kafka scaler supports Kerberos (GSSAPI) authentication for connecting to Kafka clusters secured with Kerberos. This authentication method is commonly used in enterprise environments for secure, single sign-on access to Kafka brokers.

## Prerequisites

Before configuring KEDA to use Kerberos authentication with Kafka, ensure you have:

1. **Kerberos KDC (Key Distribution Center)**: A running Kerberos KDC that manages authentication
2. **Kafka Cluster**: A Kafka cluster configured to use SASL/GSSAPI authentication
3. **Service Principal**: A service principal created for KEDA in your Kerberos realm
4. **Credentials**: Either:
   - A keytab file for the service principal (recommended for production)
   - Username and password for the principal (suitable for testing)
5. **Kerberos Configuration**: A `krb5.conf` file with your realm and KDC information

## Authentication Methods

The Kafka scaler supports two Kerberos authentication methods:

### 1. Keytab-based Authentication (Recommended)

Keytab files contain encrypted keys and are the recommended method for production environments because:
- No passwords need to be stored
- Can be easily rotated
- More secure than password-based authentication

### 2. Password-based Authentication

Uses a username and password for authentication. Suitable for:
- Development and testing environments
- Scenarios where keytab management is not feasible

## Configuration

### Step 1: Create Kerberos Credentials Secret

#### For Keytab-based Authentication

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-kerberos-credentials
  namespace: your-namespace
type: Opaque
stringData:
  sasl: "gssapi"
  username: "keda-service"  # Principal name without realm
  realm: "EXAMPLE.COM"      # Your Kerberos realm
  kerberosServiceName: "kafka"  # Kafka service principal name (default: "kafka")
  keytab: |
    <base64-encoded-keytab-file-content>
  kerberosConfig: |
    [libdefaults]
        default_realm = EXAMPLE.COM
        dns_lookup_realm = false
        dns_lookup_kdc = false
        ticket_lifetime = 24h
        renew_lifetime = 7d
        forwardable = true

    [realms]
        EXAMPLE.COM = {
            kdc = kdc.example.com:88
            admin_server = kdc.example.com:749
        }

    [domain_realm]
        .example.com = EXAMPLE.COM
        example.com = EXAMPLE.COM
```

**Note**: To get the base64-encoded keytab content:
```bash
base64 < your-keytab.keytab
```

#### For Password-based Authentication

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-kerberos-credentials
  namespace: your-namespace
type: Opaque
stringData:
  sasl: "gssapi"
  username: "keda-service"  # Principal name without realm
  password: "your-password"
  realm: "EXAMPLE.COM"
  kerberosServiceName: "kafka"
  kerberosConfig: |
    [libdefaults]
        default_realm = EXAMPLE.COM
        dns_lookup_realm = false
        dns_lookup_kdc = false
        ticket_lifetime = 24h
        renew_lifetime = 7d
        forwardable = true

    [realms]
        EXAMPLE.COM = {
            kdc = kdc.example.com:88
            admin_server = kdc.example.com:749
        }

    [domain_realm]
        .example.com = EXAMPLE.COM
        example.com = EXAMPLE.COM
```

### Step 2: Create TriggerAuthentication

```yaml
apiVersion: keda.sh/v1alpha1
kind: TriggerAuthentication
metadata:
  name: kafka-kerberos-auth
  namespace: your-namespace
spec:
  secretTargetRef:
    - parameter: sasl
      name: kafka-kerberos-credentials
      key: sasl
    - parameter: username
      name: kafka-kerberos-credentials
      key: username
    - parameter: realm
      name: kafka-kerberos-credentials
      key: realm
    - parameter: kerberosConfig
      name: kafka-kerberos-credentials
      key: kerberosConfig
    - parameter: kerberosServiceName
      name: kafka-kerberos-credentials
      key: kerberosServiceName
    # For keytab authentication:
    - parameter: keytab
      name: kafka-kerberos-credentials
      key: keytab
    # OR for password authentication:
    # - parameter: password
    #   name: kafka-kerberos-credentials
    #   key: password
```

### Step 3: Create ScaledObject

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: kafka-consumer-scaledobject
  namespace: your-namespace
spec:
  scaleTargetRef:
    name: kafka-consumer-deployment
  pollingInterval: 30
  cooldownPeriod: 300
  minReplicaCount: 0
  maxReplicaCount: 10
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: kafka.example.com:9092
      consumerGroup: my-consumer-group
      topic: my-topic
      lagThreshold: '10'
      # Optional: version of Kafka (default: 1.0.0)
      version: '2.8.0'
    authenticationRef:
      name: kafka-kerberos-auth
```

## Advanced Configuration

### Disabling FAST (Flexible Authentication Secure Tunneling)

Some Kerberos KDC implementations may not support FAST. If you encounter authentication errors related to FAST, you can disable it:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-kerberos-credentials
  namespace: your-namespace
type: Opaque
stringData:
  sasl: "gssapi"
  username: "keda-service"
  password: "your-password"
  realm: "EXAMPLE.COM"
  kerberosServiceName: "kafka"
  kerberosDisableFAST: "true"  # Add this line
  kerberosConfig: |
    [libdefaults]
        default_realm = EXAMPLE.COM
        # ... rest of config
```

Then add it to your TriggerAuthentication:

```yaml
spec:
  secretTargetRef:
    # ... other parameters
    - parameter: kerberosDisableFAST
      name: kafka-kerberos-credentials
      key: kerberosDisableFAST
```

### Custom Kerberos Service Name

If your Kafka brokers use a service principal name other than "kafka", specify it:

```yaml
stringData:
  kerberosServiceName: "customkafka"
```

### Using TLS with Kerberos

You can combine Kerberos authentication with TLS encryption:

```yaml
stringData:
  tls: "enable"
  ca: |
    -----BEGIN CERTIFICATE-----
    <your-ca-certificate>
    -----END CERTIFICATE-----
  # Optional client certificates
  cert: |
    -----BEGIN CERTIFICATE-----
    <your-client-certificate>
    -----END CERTIFICATE-----
  key: |
    -----BEGIN PRIVATE KEY-----
    <your-private-key>
    -----END PRIVATE KEY-----
```

## Complete Example

See the `examples/kafka-kerberos/` directory for a complete working example that includes:
- Kerberos KDC deployment
- Kafka broker with Kerberos authentication
- KEDA ScaledObject configuration
- Sample consumer deployment

## Troubleshooting

### Common Issues

#### 1. Clock Skew Errors

**Error**: `KDC has no support for encryption type`

**Solution**: Ensure all systems (KDC, Kafka brokers, KEDA pods) have synchronized clocks. Clock skew of more than 5 minutes will cause authentication failures.

```bash
# Check time on your cluster nodes
kubectl run -it --rm debug --image=busybox --restart=Never -- date

# Ensure NTP is configured on all nodes
```

#### 2. Principal Not Found

**Error**: `Client not found in Kerberos database`

**Solution**: Verify the principal exists in the KDC:

```bash
# On KDC server
kadmin.local -q "listprincs"
```

Ensure the principal name in your Secret matches exactly (case-sensitive).

#### 3. Keytab Permission Issues

**Error**: `Permission denied` when reading keytab

**Solution**: Ensure the keytab is properly base64-encoded and the Secret is correctly mounted. KEDA creates temporary files in `/tmp/kerberos` which must be writable.

#### 4. Wrong Realm or KDC Configuration

**Error**: `Cannot find KDC for realm`

**Solution**: Verify your `krb5.conf` is correct:
- Realm name matches your KDC
- KDC hostname/IP is accessible from KEDA pods
- Ports 88 (KDC) and 749 (kadmin) are open

#### 5. FAST-related Errors

**Error**: `Pre-authentication failed: Unsupported encryption type`

**Solution**: Try disabling FAST by setting `kerberosDisableFAST: "true"` in your Secret.

#### 6. Service Principal Mismatch

**Error**: `Server not found in Kerberos database`

**Solution**: Ensure:
- Kafka broker's service principal exists in KDC
- `kerberosServiceName` matches the Kafka service principal name
- Kafka broker's hostname is resolvable and matches the principal

### Debug Steps

1. **Verify KEDA can reach the KDC**:
```bash
kubectl exec -it <keda-operator-pod> -- nc -zv kdc.example.com 88
```

2. **Check KEDA operator logs**:
```bash
kubectl logs -n keda deployment/keda-operator -f
```

3. **Verify Secret is correctly created**:
```bash
kubectl get secret kafka-kerberos-credentials -o yaml
```

4. **Test Kafka connectivity with kinit**:
```bash
# Create a test pod with Kerberos tools
kubectl run -it --rm krb-test --image=ubuntu:22.04 -- bash

# Inside the pod:
apt-get update && apt-get install -y krb5-user kafka-clients
# Copy your krb5.conf and keytab
kinit -kt /path/to/keytab principal@REALM
kafka-console-consumer --bootstrap-server kafka.example.com:9092 \
  --topic test-topic \
  --consumer-property sasl.mechanism=GSSAPI \
  --consumer-property security.protocol=SASL_PLAINTEXT \
  --consumer-property sasl.kerberos.service.name=kafka
```

## Security Best Practices

1. **Use Keytabs in Production**: Never use password authentication in production environments
2. **Rotate Keytabs Regularly**: Implement a keytab rotation strategy
3. **Limit Principal Permissions**: Create dedicated principals for KEDA with minimal required permissions
4. **Use TLS**: Enable TLS encryption in addition to Kerberos authentication for defense-in-depth
5. **Secure Secrets**: Use encryption at rest for Kubernetes Secrets containing keytabs
6. **Monitor Authentication**: Set up alerts for authentication failures
7. **Network Segmentation**: Restrict network access to KDC and Kafka brokers

## Kerberos Configuration Reference

### Required Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `sasl` | Must be set to "gssapi" | `gssapi` |
| `username` | Principal name (without realm) | `keda-service` |
| `realm` | Kerberos realm | `EXAMPLE.COM` |
| `kerberosConfig` | Full krb5.conf content | See examples above |

### Authentication Method (choose one)

| Parameter | Description | Example |
|-----------|-------------|---------|
| `keytab` | Base64-encoded keytab file | `<base64-content>` |
| `password` | Principal password | `secretpassword` |

### Optional Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `kerberosServiceName` | Kafka service principal name | `kafka` |
| `kerberosDisableFAST` | Disable FAST | `false` |

## Additional Resources

- [KEDA Kafka Scaler Documentation](https://keda.sh/docs/scalers/apache-kafka/)
- [Apache Kafka Security Documentation](https://kafka.apache.org/documentation/#security)
- [MIT Kerberos Documentation](https://web.mit.edu/kerberos/krb5-latest/doc/)
- [Confluent Kafka Kerberos Guide](https://docs.confluent.io/platform/current/kafka/authentication_sasl/authentication_sasl_gssapi.html)

## Contributing

If you encounter issues or have improvements to this documentation, please:
1. Check existing issues at https://issues.redhat.com/browse/AUTOSCALE
2. Open a new issue with detailed information
3. Submit a pull request with documentation improvements

## Related Issues

- AUTOSCALE-285: Initial Kerberos functionality verification and testing
- OCPBUGS-60153: Customer-reported Kerberos issues
