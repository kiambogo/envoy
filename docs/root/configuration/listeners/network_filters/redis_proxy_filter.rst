.. _config_network_filters_redis_proxy:

Redis proxy
===========

* Redis :ref:`architecture overview <arch_overview_redis>`
* This filter should be configured with the type URL ``type.googleapis.com/envoy.extensions.filters.network.redis_proxy.v3.RedisProxy``.
* :ref:`v3 API reference <envoy_v3_api_msg_extensions.filters.network.redis_proxy.v3.RedisProxy>`

.. _config_network_filters_redis_proxy_stats:

Statistics
----------

Every configured Redis proxy filter has statistics rooted at *redis.<stat_prefix>.* with the
following statistics:

.. csv-table::
  :header: Name, Type, Description
  :widths: 1, 1, 2

  downstream_cx_active, Gauge, Total active connections
  downstream_cx_protocol_error, Counter, Total protocol errors
  downstream_cx_rx_bytes_buffered, Gauge, Total received bytes currently buffered
  downstream_cx_rx_bytes_total, Counter, Total bytes received
  downstream_cx_total, Counter, Total connections
  downstream_cx_tx_bytes_buffered, Gauge, Total sent bytes currently buffered
  downstream_cx_tx_bytes_total, Counter, Total bytes sent
  downstream_cx_drain_close, Counter, Number of connections closed due to draining
  downstream_rq_active, Gauge, Total active requests
  downstream_rq_total, Counter, Total requests


Splitter statistics
-------------------

The Redis filter will gather statistics for the command splitter in the
*redis.<stat_prefix>.splitter.* with the following statistics:

.. csv-table::
  :header: Name, Type, Description
  :widths: 1, 1, 2

  invalid_request, Counter, Number of requests with an incorrect number of arguments
  unsupported_command, Counter, Number of commands issued which are not recognized by the command splitter

Per command statistics
----------------------

The Redis filter will gather statistics for commands in the
*redis.<stat_prefix>.command.<command>.* namespace. By default latency stats are in milliseconds and can be
changed to microseconds by setting the configuration parameter :ref:`latency_in_micros <envoy_v3_api_field_extensions.filters.network.redis_proxy.v3.RedisProxy.latency_in_micros>` to true.

.. csv-table::
  :header: Name, Type, Description
  :widths: 1, 1, 2

  total, Counter, Number of commands
  success, Counter, Number of commands that were successful
  error, Counter, Number of commands that returned a partial or complete error response
  latency, Histogram, Command execution time in milliseconds (including delay faults)
  error_fault, Counter, Number of commands that had an error fault injected
  delay_fault, Counter, Number of commands that had a delay fault injected

.. _config_network_filters_redis_proxy_per_command_stats:

Runtime
-------

The Redis proxy filter supports the following runtime settings:

redis.drain_close_enabled
  % of connections that will be drain closed if the server is draining and would otherwise
  attempt a drain close. Defaults to 100.

.. _config_network_filters_redis_proxy_fault_injection:

Fault Injection
---------------

The Redis filter can perform fault injection. Currently, Delay and Error faults are supported.
Delay faults delay a request, and Error faults respond with an error. Moreover, errors can be delayed.

Note that the Redis filter does not check for correctness in your configuration - it is the user's
responsibility to make sure both the default and runtime percentages are correct! This is because
percentages can be changed during runtime, and validating correctness at request time is expensive.
If multiple faults are specified, the fault injection percentage should not exceed 100% for a given
fault and Redis command combination. For example, if two faults are specified; one applying to GET at 60
%, and one applying to all commands at 50%, that is a bad configuration as GET now has 110% chance of
applying a fault. This means that every request will have a fault.

If a delay is injected, the delay is additive - if the request took 400ms and a delay of 100ms
is injected, then the total request latency is 500ms. Also, due to implementation of the redis protocol,
a delayed request will delay everything that comes in after it, due to the proxy's need to respect the
order of commands it receives.

Note that faults must have a ``fault_enabled`` field, and are not enabled by default (if no default value
or runtime key are set).

Example configuration:

.. code-block:: yaml

  faults:
  - fault_type: ERROR
    fault_enabled:
      default_value:
        numerator: 10
        denominator: HUNDRED
      runtime_key: "bogus_key"
      commands:
      - GET
    - fault_type: DELAY
      fault_enabled:
        default_value:
          numerator: 10
          denominator: HUNDRED
        runtime_key: "bogus_key"
      delay: 2s

This creates two faults- an error, applying only to GET commands at 10%, and a delay, applying to all
commands at 10%. This means that 20% of GET commands will have a fault applied, as discussed earlier.

DNS lookups on redirections
---------------------------

As noted in the :ref:`architecture overview <arch_overview_redis>`, when Envoy sees a MOVED or ASK response containing a hostname it will not perform a DNS lookup and instead bubble up the error to the client. The following configuration example enables DNS lookups on such responses to avoid the client error and have Envoy itself perform the redirection:

.. code-block:: yaml

  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.network.redis_proxy.v3.RedisProxy
    stat_prefix: redis_stats
    prefix_routes:
      catch_all_route:
        cluster: cluster_0
    settings:
      op_timeout: 5
      enable_redirection: true
      dns_cache_config:
        name: dns_cache_for_redis
        dns_lookup_family: V4_ONLY
        max_hosts: 100

IAM Authentication for AWS ElastiCache
--------------------------------------

The Redis proxy filter supports IAM (Identity and Access Management) authentication when connecting
to AWS ElastiCache for Redis clusters that are configured to use IAM authentication.
This is configured using the ``iam_auth`` field within the main ``RedisProxy`` configuration.

.. code-block:: yaml

  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.network.redis_proxy.v3.RedisProxy
    stat_prefix: redis_stats
    prefix_routes:
      catch_all_route:
        cluster: my_elasticache_cluster
    settings:
      op_timeout: 1s
      # IAM Authentication Configuration
      iam_auth:
        redis_user: "my_iam_redis_user"
        cache_name: "my-production-cache" # Or an FQDN like "my-prod-cache.xxxxxx.us-east-1.cache.amazonaws.com"

``iam_auth``
  (:ref:`config.network_filters.redis_proxy.v3.RedisProxy.RedisIAMAuth <envoy_v3_api_msg_extensions.filters.network.redis_proxy.v3.RedisIAMAuth>`)
  Optional configuration for IAM authentication. If provided, Envoy will attempt to authenticate
  to the Redis upstream using AWS IAM.

  ``redis_user``
    (string, REQUIRED) The Redis username configured for IAM authentication on the ElastiCache
    cluster (e.g., the User ID of an ElastiCache user). This user must be granted the necessary
    permissions to connect to the cluster.

  ``cache_name``
    (string, REQUIRED) The identifier for the ElastiCache for Redis resource. This value helps
    determine the AWS region for signing the IAM authentication request. It can be:
      * The ElastiCache cluster name (e.g., ``my-cluster``).
      * A primary or reader endpoint FQDN (e.g., ``my-cluster.xxxxxx.us-east-1.cache.amazonaws.com``).
      * A replication group ID.

    The AWS region is determined in the following order of precedence:
      1. Environment variable ``AWS_REGION``.
      2. Environment variable ``AWS_DEFAULT_REGION``.
      3. Parsing the region from the ``cache_name`` if it is a fully qualified domain name
         (e.g., from ``my-cluster.xxxxxx.us-east-1.cache.amazonaws.com``, ``us-east-1`` would be parsed).
      4. If running on AWS infrastructure (like EC2, ECS, EKS with an IAM role attached), the AWS SDK
         may automatically discover the region from the instance/task metadata.
    If the region cannot be determined through these methods, IAM authentication may fail. It is
    recommended to either use a region-specific FQDN for ``cache_name`` or set ``AWS_REGION``/``AWS_DEFAULT_REGION``
    in Envoy's environment.

AWS Credentials
'''''''''''''''''
For IAM authentication to succeed, the Envoy instance (or its underlying host/task role) must
possess valid AWS credentials. These credentials are automatically sourced by the AWS SDK for C++
using its default credential provider chain. This includes, but is not limited to:
  - IAM roles for EC2 instances.
  - IAM roles for ECS tasks.
  - IAM roles for EKS service accounts (IRSA).
  - Environment variables (``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, and optionally ``AWS_SESSION_TOKEN``).
  - Shared credentials file (``~/.aws/credentials``) or config file (``~/.aws/config``).

Required IAM Permissions
''''''''''''''''''''''''
The AWS identity (user or role) whose credentials Envoy is using must have the necessary IAM
permissions to connect to the ElastiCache resource. The primary permission required is:
  - ``elasticache:Connect``

This permission should be granted for the specific ElastiCache user that Envoy will authenticate as.
An example policy statement might look like:

.. code-block:: json

  {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "elasticache:Connect"
        ],
        "Resource": [
          "arn:aws:elasticache:<region>:<account-id>:user:<redis_user_name>"
        ]
      }
    ]
  }

Replace ``<region>``, ``<account-id>``, and ``<redis_user_name>`` with appropriate values.
The resource ARN might also target the cluster or replication group depending on the specific
ElastiCache user type and configuration. Refer to the `AWS ElastiCache documentation for IAM <https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/IAM.Redis.html>`_
for the most up-to-date details on permissions.
