# FunNow
funnow programs
SELECT  (
        SELECT COUNT(*)
        FROM  ProductDetailViewed
        WHERE ProductDetailViewed.CreatedAt BETWEEN {{startTime}} AND {{EndTime}}
        ) AS count1,
        (
        SELECT COUNT(*)
        FROM   TimeSelectionStarted
        WHERE TimeSelectionStarted.CreatedAt BEtween {{startTime}} AND {{EndTime}}
        ) AS count2,
        (
        SELECT COUNT(*)
        FROM   TimeSelectionCompleted
        WHERE TimeSelectionCompleted.CreatedAt BEtween {{startTime}} AND {{EndTime}}
        ) AS count3,
        (
        SELECT COUNT(*)
        FROM   OrderCompleted
        WHERE OrderCompleted.CreatedAt BEtween {{startTime}} AND {{EndTime}}
        ) AS count4
FROM    dual
