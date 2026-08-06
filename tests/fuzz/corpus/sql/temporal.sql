SELECT l.a, r.b FROM t AS l ASOF LEFT JOIN t AS r ON l.a = r.a AND r.ts <= l.ts LATEST BY a ORDER BY l.ts
