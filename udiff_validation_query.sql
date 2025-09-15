-- NSE F&O UDiFF Validation Query
-- Columns ordered exactly as per NSE UDiFF source specification
-- Date: 2025-09-14 02:12:03

SELECT 
    trade_date AS TradDt,
    BizDt,
    Sgmt,
    Src,
    instrument AS FinInstrmTp,
    FinInstrmId,
    ISIN,
    symbol AS TckrSymb,
    SctySrs,
    expiry_date AS XpryDt,
    FininstrmActlXpryDt,
    strike_price AS StrkPric,
    option_type AS OptnTp,
    FinInstrmNm,
    open_price AS OpnPric,
    high_price AS HghPric,
    low_price AS LwPric,
    close_price AS ClsPric,
    LastPric,
    PrvsClsgPric,
    UndrlygPric,
    settle_price AS SttlmPric,
    open_interest AS OpnIntrst,
    change_in_oi AS ChngInOpnIntrst,
    contracts_traded AS TtlTradgVol,
    value_in_lakh AS TtlTrfVal,
    TtlNbOfTxsExctd,
    SsnId,
    NewBrdLotQty,
    Rmks,
    Rsvd1,
    Rsvd2,
    Rsvd3,
    Rsvd4
FROM step04_fo_udiff_daily
WHERE trade_date BETWEEN '20250203' AND '20250215'
ORDER BY trade_date, id;
