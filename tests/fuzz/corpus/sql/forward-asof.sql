SELECT l.a FROM t AS l ASOF JOIN t AS r ON future.a = r.a AND r.ts <= future.ts ASOF JOIN t AS future ON l.a = future.a AND future.ts <= l.ts
