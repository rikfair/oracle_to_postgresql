
CREATE FUNCTION %%schema%%.nvl(
    a VARCHAR,
    b VARCHAR
) RETURNS VARCHAR AS $$
BEGIN
    RETURN COALESCE(a, b);
END; $$
LANGUAGE plpgsql
;
