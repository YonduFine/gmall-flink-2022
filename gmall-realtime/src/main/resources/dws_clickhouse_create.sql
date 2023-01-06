create database gmall_2212;
use gmall_2212;

-- 1.流量域来源关键词粒度页面浏览各窗口汇总表
drop table if exists dws_traffic_source_keyword_page_view_window;
create table if not exists dws_traffic_source_keyword_page_view_window
(
    stt           DateTime,
    edt           DateTime,
    source        String,
    keyword       String,
    keyword_count UInt64,
    ts            UInt64
) engine = ReplacingMergeTree(ts)
    partition by toYYYYMMDD(stt)
    order by (stt, edt, source, keyword);

-- 2.流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表
drop table if exists dws_traffic_vc_ch_ar_is_new_page_view_window;
create table if not exists dws_traffic_vc_ch_ar_is_new_page_view_window
(
    stt     DateTime,
    edt     DateTime,
    vc      String,
    ch      String,
    ar      String,
    is_new  String,
    uv_ct   UInt64,
    sv_ct   UInt64,
    pv_ct   UInt64,
    dur_sum UInt64,
    uj_ct   UInt64,
    ts      UInt64
) engine = ReplacingMergeTree(ts)
    partition by toYYYYMMDD(stt)
    order by (stt, edt, vc, ch, ar, is_new);

-- 3.流量域页面浏览各窗口汇总表
drop table if exists dws_traffic_page_view_window;
create table if not exists dws_traffic_page_view_window
(
    stt               DateTime,
    edt               DateTime,
    home_uv_ct        UInt64,
    good_detail_uv_ct UInt64,
    ts                UInt64
) engine = ReplacingMergeTree(ts)
    partition by toYYYYMMDD(stt)
    order by (stt, edt);

--4.用户域用户登陆各窗口汇总表
drop table if exists dws_user_user_login_window;
create table if not exists gmall_2212.dws_user_user_login_window
(
    stt     DateTime,
    edt     DateTime,
    back_ct UInt64,
    uu_ct   UInt64,
    ts      UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt);

-- 5.用户域用户注册各窗口汇总表
drop table if exists gmall_2212.dws_user_user_register_window;
create table if not exists gmall_2212.dws_user_user_register_window
(
    stt         DateTime,
    edt         DateTime,
    register_ct UInt64,
    ts          UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt);

-- 6.统计每日各窗口加购独立用户数
drop table if exists gmall_2212.dws_trade_cart_add_uu_window;
create table if not exists gmall_2212.dws_trade_cart_add_uu_window
(
    stt            DateTime,
    edt            DateTime,
    cart_add_uu_ct UInt64,
    ts             UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt);

-- 7.交易域支付各窗口汇总表--统计支付成功独立用户数和首次支付成功用户数
drop table if exists gmall_2212.dws_trade_payment_suc_window;
create table if not exists gmall_2212.dws_trade_payment_suc_window
(
    stt                           DateTime,
    edt                           DateTime,
    payment_suc_unique_user_count UInt64,
    payment_new_user_count        UInt64,
    ts                            UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt);

-- 8.交易域下单各窗口汇总表--统计当日下单独立用户数和新增下单用户数
drop table if exists gmall_2212.dws_trade_order_window;
create table if not exists gmall_2212.dws_trade_order_window
(
    stt                          DateTime,
    edt                          DateTime,
    order_unique_user_count      UInt64,
    order_new_user_count         UInt64,
    order_activity_reduce_amount Decimal(38, 20),
    order_coupon_reduce_amount   Decimal(38, 20),
    order_origin_total_amount    Decimal(38, 20),
    ts                           UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt);

-- 9.SPU粒度下单各窗口汇总表
drop table if exists gmall_2212.dws_trade_user_spu_order_window;
create table if not exists gmall_2212.dws_trade_user_spu_order_window
(
    stt            DateTime,
    edt            DateTime,
    trademark_id   String,
    trademark_name String,
    category1_id   String,
    category1_name String,
    category2_id   String,
    category2_name String,
    category3_id   String,
    category3_name String,
    user_id        String,
    spu_id         String,
    spu_name       String,
    order_count    UInt64,
    order_amount   Decimal(38, 20),
    ts             UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt, spu_id, spu_name, user_id);

-- 10.交易域省份粒度下单各窗口汇总表
drop table if exists gmall_2212.dws_trade_province_order_window;
create table if not exists gmall_2212.dws_trade_province_order_window
(
    stt           DateTime,
    edt           DateTime,
    province_id   String,
    province_name String,
    order_count   UInt64,
    order_amount  Decimal(38, 20),
    ts            UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt, province_id);

-- 11.交易域品牌-品类-用户粒度退单各窗口汇总表
drop table if exists gmall_2212.dws_trade_trademark_category_user_refund_window;
create table if not exists gmall_2212.dws_trade_trademark_category_user_refund_window
(
    stt            DateTime,
    edt            DateTime,
    trademark_id   String,
    trademark_name String,
    category1_id   String,
    category1_name String,
    category2_id   String,
    category2_name String,
    category3_id   String,
    category3_name String,
    user_id        String,
    refund_count   UInt64,
    ts             UInt64
) engine = ReplacingMergeTree(ts)
      partition by toYYYYMMDD(stt)
      order by (stt, edt, trademark_id, trademark_name, category1_id,
                category1_name, category2_id, category2_name, category3_id, category3_name, user_id);
