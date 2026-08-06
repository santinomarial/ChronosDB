SELECT l.a, r.b FROM t AS l LATEST BY (a) ON l.ts ASOF LEFT JOIN t AS r ON l.a = r.a AND r.ts <= l.ts ORDER BY l.ts
