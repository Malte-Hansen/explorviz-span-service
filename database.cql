CREATE KEYSPACE IF NOT EXISTS explorviz WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

// Find spans in a specific time range (used to find aggregate call counts)
CREATE TABLE explorviz.span_by_time
(
    landscape_token uuid,
    start_time_s    int,
    start_time_ns   int,
    hash_code       bigint,
    span_id         bigint,
    trace_id        bigint, // to be able to select full trace from span_by_traceid?
    PRIMARY KEY ((landscape_token, start_time_s), start_time_ns, hash_code, span_id)
);

// SELECT trace_id FROM traceid_by_time WHERE landscape_token = X AND start_time_s = X;

// Get complete traces by their trace id
CREATE TABLE explorviz.span_by_traceid
(
    landscape_token uuid,
    trace_id        bigint,
    span_id         bigint,
    parent_span_id  bigint,
    start_time      bigint,
    end_time        bigint,
    hash_code       bigint,
    PRIMARY KEY ((landscape_token, trace_id), span_id) // parent_span_id as first clustering key to find parent=0 quickly?
);

// Find traces that contain a span with a certain hashcode
CREATE TABLE explorviz.trace_by_hashcode
(
    landscape_token uuid,
    hash_code       bigint,
    time_bucket     int,
    trace_id        bigint,
    PRIMARY KEY ((landscape_token, hash_code, time_bucket), trace_id)
);

//SELECT trace_id FROM traceid_by_hashcode WHERE landscape_token = X AND hash_code = X AND time_bucket = TODAY();

// Find traces by time
CREATE TABLE explorviz.trace_by_time
(
    landscape_token uuid,
    start_time_s    int,
    start_time_ns   int,
    trace_id        bigint,
    PRIMARY KEY ((landscape_token, start_time_s), start_time_ns, trace_id)
);

//SELECT * FROM span_by_traceid WHERE landscape_token = X AND trace_id IN (previous result);

// Map hashcode to metadata
CREATE TABLE explorviz.span_structure
(
    landscape_token      uuid,
    hash_code            bigint,
    host_ip_address      text,
    application_name     text,
    application_language text,
    instance_id          int,
    method_fqn           text,
    time_seen            bigint,
    PRIMARY KEY ((landscape_token), hash_code)
);
