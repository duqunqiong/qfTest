show databases ;
create database if not exists ods_nshop;
use ods_nshop;
show tables ;


create table if not exists nshop.customer
(
    customer_id         varchar(20) NOT NULL COMMENT "用户ID",
    customer_login      varchar(20) NOT NULL COMMENT "用户登录名",
    customer_nickname   varchar(10) NOT NULL COMMENT "用户名(昵称)",
    customer_name       varchar(10) NOT NULL COMMENT "用户真实姓名",
    customer_pass       varchar(8)  NOT NULL COMMENT "用户密码",
    customer_mobile     varchar(20) NOT NULL COMMENT "用户手机",
    customer_idcard     varchar(20) NOT NULL COMMENT "身份证",
    customer_gender     TINYINT     NOT NULL COMMENT "性别：1男 0女",
    customer_birthday   varchar(10) NOT NULL COMMENT "出生年月",
    customer_age        TINYINT     NOT NULL COMMENT "年龄",
    customer_age_range  varchar(2)  NOT NULL COMMENT "年龄段",
    customer_email      varchar(50)  COMMENT "用户邮箱",
    customer_natives    varchar(10)  COMMENT "所在地区",
    customer_ctime      bigint       COMMENT "创建时间",
    customer_utime      bigint       COMMENT "修改时间",
    customer_device_num varchar(20) NOT NULL COMMENT "用户手机设备号"
);

-- ENGINE = InnoDB DEFAULT CHARSET = utf8;


create table if not exists nshop.customer_consignee
(
    consignee_id      varchar(20)  NOT NULL COMMENT '收货地址ID',
    customer_id       varchar(20)  NOT NULL COMMENT '用户ID',
    consignee_name    varchar(10)  NOT NULL COMMENT '收货人',
    consignee_mobile  varchar(15)  NOT NULL COMMENT '收货人电话',
    consignee_zipcode varchar(10)  NOT NULL COMMENT '收货人地区',
    consignee_addr    varchar(200) NOT NULL COMMENT '收货人详细地址',
    consignee_tag     varchar(10)  NOT NULL COMMENT '标签：1家 2公司 3学校',
    ctime             bigint       COMMENT '创建时间',
    PRIMARY KEY (`consignee_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


create table if not exists nshop.category
(
    category_code      VARCHAR(10) NOT NULL COMMENT '分类编码',
    category_name      VARCHAR(10) NOT NULL COMMENT '分类名称',
    category_parent_id VARCHAR(10) NULL COMMENT '父分类ID',
    category_status    TINYINT     NOT NULL DEFAULT 1 COMMENT '分类状态：0禁止，1启用',
    category_utime     bigint      NULL COMMENT '最后修改时间',
    PRIMARY KEY (`category_code`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


create table if not exists nshop.supplier
(
    supplier_code   varchar(10) NOT NULL COMMENT '供应商编码',
    supplier_name   varchar(30) NOT NULL COMMENT '供应商名称',
    supplier_type   TINYINT     NOT NULL COMMENT '供应商类型：1.自营，2.官方 3其他',
    supplier_status TINYINT     NOT NULL DEFAULT 1 COMMENT '状态：0禁止，1启用',
    supplier_utime  bigint      NULL COMMENT '最后修改时间',
    PRIMARY KEY (`supplier_code`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


create table if not exists nshop.product
(
    product_code           VARCHAR(20)       NOT NULL COMMENT '商品ID(分类编码+供应商编码+编号)',
    product_name           VARCHAR(20)       NOT NULL COMMENT '商品名称',
    product_remark         TEXT              NOT NULL COMMENT '商品描述',
    category_code          VARCHAR(10)       NOT NULL COMMENT '分类ID',
    supplier_code          varchar(10)       NOT NULL COMMENT '商品的供应商编码',
    product_price          DECIMAL(5, 1)     NOT NULL COMMENT '商品销售价格',
    product_weighing_cost  DECIMAL(2, 1)     NOT NULL COMMENT '商品加权价格',
    product_publish_status TINYINT           NOT NULL DEFAULT 0 COMMENT '上下架状态：0下架 1上架',
    product_audit_status   TINYINT           NOT NULL DEFAULT 0 COMMENT '审核状态：0未审核，1已审核',
    product_bar_code       VARCHAR(50)       NOT NULL COMMENT '国条码',
    product_weight         FLOAT             NULL COMMENT '商品重量',
    product_length         FLOAT             NULL COMMENT '商品长度',
    product_height         FLOAT             NULL COMMENT '商品高度',
    product_width          FLOAT             NULL COMMENT '商品宽度',
    product_colors         SMALLINT UNSIGNED NOT NULL COMMENT '0:白|1:赤|2:红|3:黄|4:绿|5:青|6:蓝|7:紫|8:黑|9:彩色',
    product_date           varchar(10)       NOT NULL COMMENT '生产日期',
    product_shelf_life     INT               NOT NULL COMMENT '商品有效期',
    product_ctime          bigint            NULL COMMENT '商品录入时间',
    product_utime          bigint            NULL COMMENT '最后修改时间',
    PRIMARY KEY (`product_code`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;



create table if not exists nshop.orders
(
    order_id          VARCHAR(50)    NOT NULL COMMENT '订单ID(时间+商品ID+4位随机)',
    customer_id       varchar(20)    NOT NULL COMMENT '下单用户ID',
    order_status      TINYINT        NOT NULL COMMENT '订单状态：1已提交; 2待支付 3出货中 4已发货 5已收货(完成) 6投诉 7退货',
    customer_ip       varchar(20)    NULL COMMENT '下单用户IP',
    user_longitude    varchar(20)    NULL COMMENT '用户地理：经度',
    user_latitude     varchar(20)    NULL COMMENT '用户地理：纬度',
    user_areacode     varchar(10)    NULL COMMENT '用户所在地区',
    consignee_name    varchar(10)    NOT NULL COMMENT '收货人',
    consignee_mobile  varchar(15)    NOT NULL COMMENT '收货人电话',
    consignee_zipcode varchar(10)    NOT NULL COMMENT '收货人地址',
    pay_type          varchar(5)     NULL COMMENT '支付类型：线上支付 10 网上银行 11 微信 12 支付宝 | 线下支付(货到付款) 20 ',
    pay_code          varchar(30)    NULL COMMENT '支付对应唯一标识，如微信号、支付宝号',
    pay_nettype       varchar(1)     NOT NULL COMMENT '支付网络方式：0 wifi | 1 4g | 2 3g |3 线下支付',
    district_money    DECIMAL(8, 1)  NOT NULL DEFAULT 0.0 COMMENT '优惠金额',
    shipping_money    DECIMAL(8, 1)  NOT NULL DEFAULT 0.0 COMMENT '运费金额',
    payment_money     DECIMAL(10, 1) NOT NULL DEFAULT 0.0 COMMENT '支付金额',
    order_ctime       bigint         NULL COMMENT '创建时间',
    shipping_time     bigint         NULL COMMENT '发货时间',
    receive_time      bigint         NULL COMMENT '收货时间',
    PRIMARY KEY (`order_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


create table if not exists nshop.orderdetail
(
    orderdetailid    varchar(20)   NOT NULL COMMENT '订单详情表ID',
    orderid          VARCHAR(50)   NOT NULL COMMENT '订单表ID',
    productid        varchar(20)   NOT NULL COMMENT '订单商品ID',
    productname      VARCHAR(50)   NOT NULL COMMENT '商品名称',
    productremark    VARCHAR(100)  NOT NULL COMMENT '商品描述',
    productcnt       INT           NOT NULL DEFAULT 1 COMMENT '购买商品数量',
    productprice     DECIMAL(5, 1) NOT NULL COMMENT '购买商品单价',
    weighingcost     DECIMAL(2, 1) NOT NULL COMMENT '商品加权价格',
    districtmoney    DECIMAL(4, 1) NOT NULL DEFAULT 0.0 COMMENT '优惠金额',
    isactivity                     NOT NULL DEFAULT 0 COMMENT '1:参加活动|0：没有参加活动',
    orderdetailctime bigint        NULL COMMENT '下单时间',
    PRIMARY KEY (order_detail_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


create table if not exists nshop.orders_pay_records
(
    pay_id      varchar(20)   NOT NULL COMMENT '支付记录ID',
    order_id    varchar(30)   NOT NULL COMMENT '订单ID',
    customer_id varchar(20)   NOT NULL COMMENT '用户ID',
    pay_status  varchar(5)    NOT NULL COMMENT '支付状态：0 支付失败| 1 支付成功',
    pay_type    varchar(5)    NULL COMMENT '支付类型：线上支付 10 网上银行 11 微信 12 支付宝 | 线下支付(货到付款) 20 ',
    pay_code    varchar(30)   NULL COMMENT '支付对应唯一标识，如微信号、支付宝号',
    pay_nettype varchar(1)    NOT NULL COMMENT '支付网络方式：1 wifi | 2 4g | 3 3g |4 线下支付',
    pay_amount  double(10, 1) NOT NULL COMMENT '支付金额',
    pay_ctime   bigint        NULL COMMENT '创建时间',
    PRIMARY KEY (`pay_id`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


create table if not exists nshop.date_dim
(
    date_day          varchar(30) NOT NULL COMMENT '日期：yyyyMMdd 如20190520',
    date_day_desc     varchar(30) NOT NULL COMMENT '日期格式：yyyy年MM月dd日 如2019年05月20日',
    date_day_month    varchar(30) NOT NULL COMMENT '日期：20 本月第几天 如2019年5月20日为5月第20天',
    date_day_year     varchar(30) NOT NULL COMMENT '日期：139 本年第几天 如2019年5月20日为139天',
    date_day_en       varchar(30) NOT NULL COMMENT '日期：monday 星期几',
    date_week         varchar(30) NOT NULL COMMENT '周：本月第几周 4 如 20190520为本月第4周',
    date_week_desc    varchar(30) NOT NULL COMMENT '周：本月第几周 如 20190504',
    date_month        varchar(30) NOT NULL COMMENT '月: 5 如 20190520为本年5月',
    date_month_en     varchar(30) NOT NULL COMMENT '月: may ',
    date_month_desc   varchar(30) NOT NULL COMMENT '月：如 201905',
    date_quarter      varchar(30) NOT NULL COMMENT '季度:2',
    date_quarter_en   varchar(30) NOT NULL COMMENT '季度:Q2',
    date_quarter_desc varchar(30) NOT NULL COMMENT '季度:2019Q2 如 20190520为2019Q2',
    date_year         varchar(30) NOT NULL COMMENT '年:2019',
    PRIMARY KEY (`date_day`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


create table if not exists nshop.area_dim
(
    region_code          varchar(30) NOT NULL COMMENT '地区编码 如110105 | 130406 ',
    region_code_desc     varchar(30) NOT NULL COMMENT '地区编码 如朝阳区 | 峰峰矿区',
    region_city          varchar(30) NOT NULL COMMENT '地区编码 如1101 北京市朝阳区 | 1304 邯郸',
    region_city_desc     varchar(30) NOT NULL COMMENT '地区编码 如1101 | 1304 邯郸市',
    region_province      varchar(30) NOT NULL COMMENT '地区编码 如11 北京市 | 13 河北省',
    region_province_desc varchar(30) NOT NULL COMMENT '地区编码 如 北京市 | 河北',
    PRIMARY KEY (`region_code`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;


create table if not exists nshop.comm_dim
(
    dim_type   VARCHAR(10) not NULL COMMENT '字典类型',
    dim_code   VARCHAR(30) not NULL COMMENT '字典编码',
    dim_remark varchar(30) NULL COMMENT '字段描述',
    dim_ext1 varchar(30) NULL COMMENT '扩展1',
    dim_ext2 varchar(30) NULL COMMENT '扩展2',
    dim_ext3 varchar(30) NULL COMMENT '扩展3',
    dim_ext4 varchar(30) NULL COMMENT '扩展4',
    ct timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`dim_type`,`dim_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

create table if not exists nshop.page_dim
(
    page_code   varchar(30) NOT NULL COMMENT '页面编码',
    page_remark varchar(30) NULL COMMENT '页面描述',
    page_type   varchar(5)  NOT NULL COMMENT '页面类型(1:导航页 2：分类页 3：店铺页 4：产品页)',
    page_target varchar(30) NULL COMMENT '页面对应实体编号(如产品、店铺)',
    page_ctime  bigint      NOT NULL COMMENT '创建时间',
    PRIMARY KEY (`page_code`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

-- 用户行为数据
create external table if not exists ods_nshop.ods_nshop_01_useractlog
(
    action string comment '行为类型:install安装|launch启动|interactive交互|page_enter_h5页面曝光|page_enter_native页面进入|exit退出',
    event_type string comment '行为类型:click点击|view浏览|slide滑动|input输入',
    customer_id string comment '用户id',
    device_num string comment '设备号',
    device_type string comment '设备类型',
    os string comment '手机系统',
    os_version string comment '手机系统版本',
    manufacturer string comment '手机制造商',
    carrier string comment '电信运营商',
    network_type string comment '网络类型',
    area_code string comment '地区编码',
    longitude string comment '经度',
    latitude string comment '纬度',
    extinfo string comment '扩展信息(json格式)',
    duration string comment '停留时长',
    ct bigint comment '创建时间'
)
partitioned by (bdp_day string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE location '/data/nshop/ods/user_action_log/';

-- 外部数据
create external table if not exists ods_nshop.ods_nshop_01_useractlog
(
    action string comment '行为类型:install安装|launch启动|interactive交互|page_enter_h5页面曝光|page_enter_native页面进入|exit退出', event_type string comment '行为类型:click点击|view浏览|slide滑动|input输入',
    customer_id string comment '用户id',
    device_num string comment '设备号',
    device_type string comment '设备类型',
    os string comment '手机系统',
    os_version string comment '手机系统版本',
    manufacturer string comment '手机制造商',
    carrier string comment '电信运营商',
    network_type string comment '网络类型',
    area_code string comment '地区编码',
    longitude string comment '经度',
    latitude string comment '纬度',
    extinfo string comment '扩展信息(json格式)',
    duration string comment '停留时长',
    ct bigint comment '创建时间'
)
partitioned by (bdp_day string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE location '/data/nshop/ods/user_action_log/';




