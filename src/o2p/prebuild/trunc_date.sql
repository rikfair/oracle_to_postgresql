
CREATE OR REPLACE FUNCTION %%schema%%.trunc(d ANYELEMENT)
RETURNS DATE
LANGUAGE plpgsql
AS
$$
BEGIN
  RETURN DATE_TRUNC('DAY', d);
END;
$$