{\rtf1\ansi\ansicpg1252\cocoartf2638
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\paperw11900\paperh16840\margl1440\margr1440\vieww28300\viewh14380\viewkind0
\pard\tx566\tx1133\tx1700\tx2267\tx2834\tx3401\tx3968\tx4535\tx5102\tx5669\tx6236\tx6803\pardirnatural\partightenfactor0

\f0\fs24 \cf0 WITH trades_summary AS (\
    SELECT\
        DATE_TRUNC('day', open_time) AS dt_report,\
        login_hash,\
        server_hash,\
        symbol,\
        SUM(volume) AS sum_volume_prev_7d,\
        SUM(SUM(volume)) OVER (PARTITION BY login_hash, symbol ORDER BY DATE_TRUNC('day', open_time) RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW) AS sum_volume_prev_all,\
        DENSE_RANK() OVER (PARTITION BY login_hash, symbol ORDER BY SUM(volume) DESC) AS rank_volume_symbol_prev_7d,\
        DENSE_RANK() OVER (PARTITION BY login_hash ORDER BY COUNT(*) DESC) AS rank_count_prev_7d,\
        SUM(volume) FILTER (WHERE DATE_TRUNC('month', open_time) = '2020-08-01') AS sum_volume_2020_08,\
        MIN(open_time) AS date_first_trade,\
        ROW_NUMBER() OVER (PARTITION BY login_hash, symbol ORDER BY DATE_TRUNC('day', open_time) DESC) AS row_number\
    FROM\
        trades\
    WHERE\
        open_time >= '2020-06-01' AND open_time < '2020-10-01'\
    GROUP BY\
        DATE_TRUNC('day', open_time),\
        login_hash,\
        server_hash,\
        symbol\
),\
filtered_users AS (\
    SELECT\
        login_hash,\
        currency\
    FROM\
        users\
    WHERE\
        enable = 1\
)\
SELECT\
    ts.dt_report,\
    ts.login_hash,\
    ts.server_hash,\
    ts.symbol,\
    fu.currency,\
    ts.sum_volume_prev_7d,\
    ts.sum_volume_prev_all,\
    ts.rank_volume_symbol_prev_7d,\
    ts.rank_count_prev_7d,\
    ts.sum_volume_2020_08,\
    ts.date_first_trade,\
    ts.row_number\
FROM\
    trades_summary ts\
JOIN\
    filtered_users fu\
ON\
    ts.login_hash = fu.login_hash\
ORDER BY\
    ts.row_number DESC;\
}