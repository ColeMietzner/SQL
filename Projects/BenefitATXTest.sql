WITH 
      ATXCOUNT as 
            (
                  SELECT p.name, atx.PayPeriodEndDate, COUNT(*) AS TransCount
                  FROM accrualtransaction atx
                  JOIN property p ON p.id = atx.PropertyID
                  WHERE atx.PayPeriodEndDate > '8/1/2012'  AND atx.PayPeriodEndDate < getDate()-5
                  GROUP BY p.name, atx.PayPeriodEndDate
            ),
      ATXAVERAGE AS 
            (
                  SELECT name, avg(ATXCOUNT.TransCount) as avgcount from ATXCOUNT group by name
            ), 
      CURRENTTRANS as 
            (
                  SELECT count(*) as CurrentATX, p.Name
                  FROM accrualtransaction atx
                  JOIN property p ON p.id = atx.PropertyID
                  WHERE atx.PayPeriodEndDate > getDate()-5 and atx.PayPeriodEndDate < getDate()+ 5 group by P.Name
            )

SELECT 
      a.Name,
      CASE 
      WHEN a.CurrentATX not between b.avgcount * .9 and b.avgcount * 1.1 THEN 'Accrual Transactions have Failed'
      else 'Accrual Transactions have Passed' 
      END as VALUE
FROM CURRENTTRANS a join ATXAVERAGE b on a.name = b.name







