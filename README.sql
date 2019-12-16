# FunNow
funnow programs
#請輸入優惠碼 "Code"

SELECT 表一.優惠序號, 表一.輸入量, 表二.使用量, 表一.輸入量/表二.使用量 AS 使用率 ,表二.GMV, 表二.Discount, 表三.首購GMV , 表三.UU as 首購UU,表三.首購Discount, 表四.AwakeUserBuyUU, 表四.AwakeUserBuyGMV, 表五.輸入量 as 'Awake User 輸入量'
FROM 
(
    SELECT PromoCode.Name, PromoCode.Code as 優惠序號, COUNT(PromoHistory.ID) as 輸入量
    FROM PromoCode 
    LEFT JOIN PromoHistory ON PromoCode.ID = PromoHistory.PromoCodeID
    LEFT JOIN PromoDiscount ON PromoCode.ID = PromoDiscount.PromoCodeID AND PromoHistory.UID = PromoDiscount.UID
    LEFT JOIN Orders ON PromoDiscount.OrderID = Orders.ID AND Orders.Status = 2
    WHERE PromoCode.Code = {{code}} 
) 表一
LEFT JOIN 
(
    SELECT PromoCode.Code as 優惠序號, COUNT(PromoDiscount.ID) 使用量, SUM(Orders.TotalAmount) as GMV, SUM(PromoDiscount.Discount) Discount
    FROM PromoCode 
    LEFT JOIN PromoHistory ON PromoCode.ID = PromoHistory.PromoCodeID
    LEFT JOIN PromoDiscount ON PromoCode.ID = PromoDiscount.PromoCodeID AND PromoHistory.UID = PromoDiscount.UID
    LEFT JOIN Orders ON PromoDiscount.OrderID = Orders.ID AND Orders.Status = 2
    WHERE PromoCode.Code = {{code}} 
) 表二 ON 表一.優惠序號 = 表二.優惠序號

LEFT JOIN 
(
    SELECT PromoCode.Code as 優惠序號, SUM(Orders.TotalAmount) as 首購GMV, SUM(PromoDiscount.Discount) 首購Discount, COUNT(DISTINCT PromoDiscount.UID) as UU
    FROM PromoCode 
    LEFT JOIN PromoHistory ON PromoCode.ID = PromoHistory.PromoCodeID
    LEFT JOIN PromoDiscount ON PromoCode.ID = PromoDiscount.PromoCodeID AND PromoHistory.UID = PromoDiscount.UID
    LEFT JOIN Orders ON PromoDiscount.OrderID = Orders.ID AND Orders.Status = 2
    WHERE PromoCode.Code = {{code}} 
    AND Orders.CreatedAt = (SELECT MIN(o2.CreatedAt)
                            FROM Orders o2
                            WHERE o2.UID = Orders.UID AND o2.status = 2)
) 表三 ON 表一.優惠序號 = 表三.優惠序號

LEFT JOIN 
(
 SELECT T1.Code, SUM(TotalAmount), COUNT(DISTINCT T1.UID) as 'AwakeUserBuyUU', SUM(T1.TotalAmount) as 'AwakeUserBuyGMV'
FROM(
    SELECT PromoCode.Code, Orders.ID, Member.UID, Member.CreatedAt as MemberCreateAt, Orders.CreatedAt as firstOrder, Orders.TotalAmount
    FROM PromoCode
    LEFT JOIN PromoDiscount ON PromoCode.ID = PromoDiscount.PromoCodeID
    LEFT JOIN Orders ON PromoDiscount.OrderID = Orders.ID
    LEFT JOIN Member ON Orders.UID = Member.UID
    WHERE Orders.CreatedAt = (
        SELECT MIN(o2.CreatedAt)
        FROM Orders o2
        WHERE o2.UID = Orders.UID AND o2.status = 2
    ) AND PromoCode.Code = {{code}} 
)T1
WHERE TIMESTAMPDIFF(WEEK, T1.MemberCreateAt, T1.firstOrder) BETWEEN 4 AND 26
GROUP BY T1.Code
)表四 ON 表一.優惠序號 = 表四.Code

LEFT JOIN(
SELECT T1.Code, COUNT (DISTINCT UID) as 輸入量

FROM(
    SELECT Code, PromoHistory.UID, Orders.CreatedAt AS OrderTime, Member.CreatedAt AS MemberTime
    FROM PromoCode
    LEFT JOIN PromoHistory ON PromoCode.ID = PromoHistory.PromoCodeID
    LEFT JOIN Member ON PromoHistory.UID = Member.UID
    LEFT JOIN Orders ON Member.UID = Orders.UID

WHERE Orders.CreatedAt = (
        SELECT MIN(o2.CreatedAt)
        FROM Orders o2
        WHERE o2.UID = Orders.UID AND o2.status = 2
    ) AND PromoCode.Code = {{code}}
)T1
WHERE TIMESTAMPDIFF(WEEK, T1.MemberTime, T1.OrderTime) BETWEEN 4 AND 26
GROUP BY T1.Code
)表五 ON 表一.優惠序號 = 表五.Code
