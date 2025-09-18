#!/usr/bin/env python3
"""
CocoPan SKU Data Population Script
Populates master_skus table with all GrabFood and Foodpanda SKU data
Extracted from storefront flow documents
"""

import logging
from typing import List, Dict
from database import db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_grabfood_skus() -> List[Dict]:
    """Get all GrabFood SKUs from storefront flow document"""
    return [
        # Best-Sellers Category
        {"sku_code": "GB062", "product_name": "MILKY CHEESE DONUT", "category": "BREAD", "platform": "grabfood", "gmv_q3": 1595421.82, "flow_category": "Best-Sellers", "division": "BREAD"},
        {"sku_code": "GD028", "product_name": "VIETNAMESE COFFEE", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 837763.57, "flow_category": "Best-Sellers", "division": "NON-BREAD"},
        {"sku_code": "GD113", "product_name": "MILO OVERLOAD", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 856857.15, "flow_category": "Best-Sellers", "division": "NON-BREAD"},
        {"sku_code": "GB110", "product_name": "CINNAMON ROLL DELUXE", "category": "BREAD", "platform": "grabfood", "gmv_q3": 601297.50, "flow_category": "Best-Sellers", "division": "BREAD"},
        {"sku_code": "GB001", "product_name": "PAN DE COCO", "category": "BREAD", "platform": "grabfood", "gmv_q3": 758896.86, "flow_category": "Best-Sellers", "division": "BREAD"},
        {"sku_code": "GD057", "product_name": "MATCHA MILK", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 625008.03, "flow_category": "Best-Sellers", "division": "NON-BREAD"},
        {"sku_code": "GD117", "product_name": "TWISTEA CLASSIC", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 72952.15, "flow_category": "Best-Sellers", "division": "NON-BREAD"},
        {"sku_code": "GB107", "product_name": "DOUBLE CHEESE ROLL", "category": "BREAD", "platform": "grabfood", "gmv_q3": 484849.49, "flow_category": "Best-Sellers", "division": "BREAD"},

        # Classic Favorites
        {"sku_code": "GB004", "product_name": "GRAB CHEESE ROLL", "category": "BREAD", "platform": "grabfood", "gmv_q3": 503125.58, "flow_category": "Classic Favorites", "division": "BREAD"},
        {"sku_code": "GB006", "product_name": "GRAB SPANISH BREAD", "category": "BREAD", "platform": "grabfood", "gmv_q3": 420680.43, "flow_category": "Classic Favorites", "division": "BREAD"},
        {"sku_code": "GB008", "product_name": "GRAB CHOCO ROLL", "category": "BREAD", "platform": "grabfood", "gmv_q3": 224814.50, "flow_category": "Classic Favorites", "division": "BREAD"},
        {"sku_code": "GB104", "product_name": "GRAB PAN DE SAL (10 PCS)", "category": "BREAD", "platform": "grabfood", "gmv_q3": 116562.35, "flow_category": "Classic Favorites", "division": "BREAD"},

        # Sweet Favorites
        {"sku_code": "GB028", "product_name": "GRAB CINNAMON ROLL CLASSIC", "category": "BREAD", "platform": "grabfood", "gmv_q3": 237303.49, "flow_category": "Sweet Favorites", "division": "BREAD"},
        {"sku_code": "GB069", "product_name": "GRAB COFFEE BUN", "category": "BREAD", "platform": "grabfood", "gmv_q3": 271841.20, "flow_category": "Sweet Favorites", "division": "BREAD"},
        {"sku_code": "GB103", "product_name": "GRAB BLUEBERRY MUFFIN", "category": "BREAD", "platform": "grabfood", "gmv_q3": 145822.51, "flow_category": "Sweet Favorites", "division": "BREAD"},
        {"sku_code": "GB096", "product_name": "GRAB CHOCO CHIP MUFFIN", "category": "BREAD", "platform": "grabfood", "gmv_q3": 177441.38, "flow_category": "Sweet Favorites", "division": "BREAD"},

        # Donuts
        {"sku_code": "GB074", "product_name": "GRAB CHOCO CHEESE DONUT", "category": "BREAD", "platform": "grabfood", "gmv_q3": 328551.42, "flow_category": "Donuts", "division": "BREAD"},
        {"sku_code": "GB061", "product_name": "GRAB CHEESE BURST DONUT", "category": "BREAD", "platform": "grabfood", "gmv_q3": 116089.50, "flow_category": "Donuts", "division": "BREAD"},
        {"sku_code": "GB034", "product_name": "GRAB SUGAR DONUT", "category": "BREAD", "platform": "grabfood", "gmv_q3": 257118.31, "flow_category": "Donuts", "division": "BREAD"},
        {"sku_code": "GB073", "product_name": "GRAB GLAZED DONUT", "category": "BREAD", "platform": "grabfood", "gmv_q3": 119328.00, "flow_category": "Donuts", "division": "BREAD"},
        {"sku_code": "GB105", "product_name": "GRAB CHOCO FROST DONUT", "category": "BREAD", "platform": "grabfood", "gmv_q3": 199044.07, "flow_category": "Donuts", "division": "BREAD"},
        {"sku_code": "GB106", "product_name": "GRAB STRAWBERRY SPRINKLE DONUT", "category": "BREAD", "platform": "grabfood", "gmv_q3": 138432.51, "flow_category": "Donuts", "division": "BREAD"},

        # Savory Favorites
        {"sku_code": "GB102", "product_name": "GRAB CHEESY SAUSAGE ROLL", "category": "BREAD", "platform": "grabfood", "gmv_q3": 250797.00, "flow_category": "Savory Favorites", "division": "BREAD"},
        {"sku_code": "GB095", "product_name": "GRAB CHEESY HAM ROLL", "category": "BREAD", "platform": "grabfood", "gmv_q3": 156874.75, "flow_category": "Savory Favorites", "division": "BREAD"},
        {"sku_code": "GB070", "product_name": "GRAB PAN DE FLOSS ORIGINAL", "category": "BREAD", "platform": "grabfood", "gmv_q3": 210293.15, "flow_category": "Savory Favorites", "division": "BREAD"},
        {"sku_code": "GB071", "product_name": "GRAB PAN DE FLOSS SPICY", "category": "BREAD", "platform": "grabfood", "gmv_q3": 160848.82, "flow_category": "Savory Favorites", "division": "BREAD"},
        {"sku_code": "GB088", "product_name": "GRAB ITALIAN CHEESE ROLL", "category": "BREAD", "platform": "grabfood", "gmv_q3": 156268.00, "flow_category": "Savory Favorites", "division": "BREAD"},
        {"sku_code": "GB109", "product_name": "GRAB TUNA BUN", "category": "BREAD", "platform": "grabfood", "gmv_q3": 155276.35, "flow_category": "Savory Favorites", "division": "BREAD"},
        {"sku_code": "GB111", "product_name": "GRAB K-SALT BREAD", "category": "BREAD", "platform": "grabfood", "gmv_q3": 129468.00, "flow_category": "Savory Favorites", "division": "BREAD"},
        {"sku_code": "GB112", "product_name": "GRAB CHICKEN ASADO BUN", "category": "BREAD", "platform": "grabfood", "gmv_q3": 30365.00, "flow_category": "Savory Favorites", "division": "BREAD"},

        # Daily Loaf
        {"sku_code": "GB097", "product_name": "GRAB DAILY LOAF - RAISIN", "category": "BREAD", "platform": "grabfood", "gmv_q3": 61865.00, "flow_category": "Daily Loaf", "division": "BREAD"},
        {"sku_code": "GB099", "product_name": "GRAB DAILY LOAF - ITALIAN HERB", "category": "BREAD", "platform": "grabfood", "gmv_q3": 77281.75, "flow_category": "Daily Loaf", "division": "BREAD"},

        # Coffee - Vietnamese Coffee
        {"sku_code": "GD029", "product_name": "GRAB ICED VIETNAMESE COFFEE", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 154414.29, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD083", "product_name": "GRAB ICED VIETNAMESE COFFEE MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 122307.86, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD039", "product_name": "GRAB ICED VIETNAMESE COFFEE XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 194295.00, "flow_category": "Coffee", "division": "NON-BREAD"},

        # Coffee - Cafe Espanol
        {"sku_code": "GD048", "product_name": "GRAB HOT CAFE ESPANOL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 14000.00, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD049", "product_name": "GRAB ICED CAFE ESPANOL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 99660.00, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD087", "product_name": "GRAB ICED CAFE ESPANOL MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 54927.86, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD050", "product_name": "GRAB ICED CAFE ESPANOL XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 111470.00, "flow_category": "Coffee", "division": "NON-BREAD"},

        # Coffee - Creamy Vanilla
        {"sku_code": "GD004", "product_name": "GRAB HOT CREAMY VANILLA", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 14020.00, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD008", "product_name": "GRAB ICED CREAMY VANILLA", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 106345.00, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD081", "product_name": "GRAB ICED CREAMY VANILLA MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 56325.00, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD036", "product_name": "GRAB ICED CREAMY VANILLA XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 103972.14, "flow_category": "Coffee", "division": "NON-BREAD"},

        # Coffee - Salted Caramel
        {"sku_code": "GD051", "product_name": "GRAB HOT SALTED CARAMEL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 14905.00, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD052", "product_name": "GRAB ICED SALTED CARAMEL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 79977.86, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD088", "product_name": "GRAB ICED SALTED CARAMEL MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 62567.86, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD053", "product_name": "GRAB ICED SALTED CARAMEL XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 92177.14, "flow_category": "Coffee", "division": "NON-BREAD"},

        # Coffee - Dark Chocolate
        {"sku_code": "GD033", "product_name": "GRAB HOT DARK CHOCOLATE COFFEE", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 13377.86, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD034", "product_name": "GRAB ICED DARK CHOCOLATE COFFEE", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 74610.71, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD084", "product_name": "GRAB ICED DARK CHOCOLATE COFFEE MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 60665.00, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD041", "product_name": "GRAB ICED DARK CHOCOLATE COFFEE XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 89427.14, "flow_category": "Coffee", "division": "NON-BREAD"},

        # Coffee - Signature Milky
        {"sku_code": "GD104", "product_name": "GRAB HOT SIGNATURE MILKY COFFEE", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 11840.00, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD105", "product_name": "GRAB ICED SIGNATURE MILKY COFFEE", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 40501.43, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD107", "product_name": "GRAB ICED SIGNATURE MILKY COFFEE MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 21380.00, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD106", "product_name": "GRAB ICED SIGNATURE MILKY COFFEE XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 30740.00, "flow_category": "Coffee", "division": "NON-BREAD"},

        # Coffee - Signature Black
        {"sku_code": "GD003", "product_name": "GRAB HOT SIGNATURE BLACK COFFEE", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 10735.00, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD007", "product_name": "GRAB ICED SIGNATURE BLACK COFFEE", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 23748.57, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD080", "product_name": "GRAB ICED SIGNATURE BLACK COFFEE MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 26754.28, "flow_category": "Coffee", "division": "NON-BREAD"},
        {"sku_code": "GD035", "product_name": "GRAB ICED SIGNATURE BLACK COFFEE XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 23885.72, "flow_category": "Coffee", "division": "NON-BREAD"},

        # Chocolate - Milo
        {"sku_code": "GD130", "product_name": "GRAB HOT MILO OVERLOAD", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 10280.00, "flow_category": "Chocolate", "division": "NON-BREAD"},
        {"sku_code": "GD115", "product_name": "GRAB MILO OVRLOAD ICED MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 199171.43, "flow_category": "Chocolate", "division": "NON-BREAD"},
        {"sku_code": "GD114", "product_name": "GRAB MILO OVRLOAD ICED XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 172450.00, "flow_category": "Chocolate", "division": "NON-BREAD"},

        # Chocolate - Rich Choco
        {"sku_code": "GD005", "product_name": "GRAB HOT RICH CHOCO", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 6321.56, "flow_category": "Chocolate", "division": "NON-BREAD"},
        {"sku_code": "GD009", "product_name": "GRAB ICED RICH CHOCO", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 73567.86, "flow_category": "Chocolate", "division": "NON-BREAD"},
        {"sku_code": "GD082", "product_name": "GRAB ICED RICH CHOCO MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 48020.00, "flow_category": "Chocolate", "division": "NON-BREAD"},
        {"sku_code": "GD037", "product_name": "GRAB ICED RICH CHOCO XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 67669.20, "flow_category": "Chocolate", "division": "NON-BREAD"},

        # Matcha
        {"sku_code": "GD089", "product_name": "GRAB ICED MATCHA MILK MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 96707.86, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "GD059", "product_name": "GRAB ICED MATCHA MILK XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 136146.60, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "GD058", "product_name": "GRAB ICED MATCHA STRAWBERRY", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 66500.00, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "GD090", "product_name": "GRAB ICED MATCHA STRAWBERRY MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 51626.43, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "GD060", "product_name": "GRAB ICED MATCHA STRAWBERRY XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 80163.57, "flow_category": "Matcha", "division": "NON-BREAD"},

        # Coolers
        {"sku_code": "GD067", "product_name": "GRAB LEMON BERRY COOLER", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 51446.43, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD092", "product_name": "GRAB LEMON BERRY COOLER MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 54967.86, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD068", "product_name": "GRAB LEMON BERRY COOLER XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 54906.60, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD086", "product_name": "GRAB FRUITY MELON MILK MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 48615.00, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD044", "product_name": "GRAB FRUITY MELON MILK", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 47274.29, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD047", "product_name": "GRAB FRUITY MELON MILK XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 57595.00, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD043", "product_name": "GRAB FRUITY STRAWBERRY MILK", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 50053.57, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD085", "product_name": "GRAB FRUITY STRAWBERRY MILK MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 34260.00, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD046", "product_name": "GRAB FRUITY STRAWBERRY MILK XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 53306.25, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD069", "product_name": "GRAB LEMON PASSION COOLER", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 41290.71, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD093", "product_name": "GRAB LEMON PASSION COOLER MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 37029.29, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD070", "product_name": "GRAB LEMON PASSION COOLER XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 37425.00, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD119", "product_name": "GRAB COCOPAN TWISTEA MX", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 27665.00, "flow_category": "Coolers", "division": "NON-BREAD"},
        {"sku_code": "GD118", "product_name": "GRAB COCOPAN TWISTEA XL", "category": "NON-BREAD", "platform": "grabfood", "gmv_q3": 20835.72, "flow_category": "Coolers", "division": "NON-BREAD"},
    ]

def get_foodpanda_skus() -> List[Dict]:
    """Get all Foodpanda SKUs from storefront flow document"""
    return [
        # Best-Sellers Category
        {"sku_code": "FB062", "product_name": "MILKY CHEESE DONUT", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 1595421.82, "flow_category": "Best-Sellers", "division": "BREAD"},
        {"sku_code": "FD113", "product_name": "MILO OVERLOAD", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 856857.15, "flow_category": "Best-Sellers", "division": "NON-BREAD"},
        {"sku_code": "FB107", "product_name": "DOUBLE CHEESE ROLL", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 484849.49, "flow_category": "Best-Sellers", "division": "BREAD"},
        {"sku_code": "FD117", "product_name": "TWISTEA CLASSIC", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 72952.15, "flow_category": "Best-Sellers", "division": "NON-BREAD"},
        {"sku_code": "FB001", "product_name": "PAN DE COCO", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 758896.86, "flow_category": "Best-Sellers", "division": "BREAD"},
        {"sku_code": "FD028", "product_name": "VIETNAMESE COFFEE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 837763.57, "flow_category": "Best-Sellers", "division": "NON-BREAD"},
        {"sku_code": "FB110", "product_name": "CINNAMON ROLL DELUXE", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 601297.50, "flow_category": "Best-Sellers", "division": "BREAD"},
        {"sku_code": "FD057", "product_name": "MATCHA MILK", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 625008.03, "flow_category": "Best-Sellers", "division": "NON-BREAD"},

        # Breads and Pastries
        {"sku_code": "FB002", "product_name": "FOODPANDA PAN DE SAL (SMALL)", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 1120.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB004", "product_name": "FOODPANDA CHEESE ROLL", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 296343.75, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB086", "product_name": "FOODPANDA CHOCO BANANA MUFFIN", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 30.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB006", "product_name": "FOODPANDA SPANISH BREAD", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 220361.25, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB098", "product_name": "FOODPANDA DAILY LOAF - CHOCO", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 18408.50, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB074", "product_name": "FOODPANDA CHOCO CHEESE DONUT", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 213396.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB072", "product_name": "FOODPANDA BANANA BREAD", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 19816.25, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB102", "product_name": "FOODPANDA CHEESY SAUSAGE ROLL", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 169310.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB028", "product_name": "FOODPANDA CINNAMON ROLL CLASSIC", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 163303.98, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB050", "product_name": "FOODPANDA UBE CHEESE", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 154947.90, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB034", "product_name": "FOODPANDA SUGAR DONUT", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 149651.25, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB105", "product_name": "FOODPANDA CHOCO FROST DONUT", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 148447.50, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB070", "product_name": "FOODPANDA PAN DE FLOSS ORIGINAL", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 142224.75, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB069", "product_name": "FOODPANDA COFFEE BUN", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 140737.75, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB008", "product_name": "FOODPANDA CHOCO ROLL", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 129468.50, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB071", "product_name": "FOODPANDA PAN DE FLOSS SPICY", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 124807.75, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB073", "product_name": "FOODPANDA GLAZED DONUT", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 109240.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB096", "product_name": "FOODPANDA CHOCO CHIP MUFFIN", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 108675.50, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB095", "product_name": "FOODPANDA CHEESY HAM ROLL", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 106138.75, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB103", "product_name": "FOODPANDA BLUEBERRY MUFFIN", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 101836.50, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB109", "product_name": "FOODPANDA TUNA BUN", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 99459.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB106", "product_name": "FOODPANDA STRAWBERRY SPRINKLE DONUT", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 85820.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB094", "product_name": "FOODPANDA BANANA CRUNCH", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 82515.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB088", "product_name": "FOODPANDA ITALIAN CHEESE ROLL", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 81608.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB111", "product_name": "FOODPANDA K-SALT BREAD", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 70728.25, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB104", "product_name": "FOODPANDA PAN DE SAL (10 PCS)", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 67680.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB061", "product_name": "FOODPANDA CHEESE BURST DONUT", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 65520.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB089", "product_name": "FOODPANDA MILKY BUN", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 50188.25, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB099", "product_name": "FOODPANDA DAILY LOAF - ITALIAN HERB", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 43260.00, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB097", "product_name": "FOODPANDA DAILY LOAF - RAISIN", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 37931.25, "flow_category": "Breads and Pastries", "division": "BREAD"},
        {"sku_code": "FB112", "product_name": "FOODPANDA CHICKEN ASADO BUN", "category": "BREAD", "platform": "foodpanda", "gmv_q3": 22495.00, "flow_category": "Breads and Pastries", "division": "BREAD"},

        # Hot Coffee
        {"sku_code": "FD097", "product_name": "FOODPANDA HOT HAZELNUT COFFEE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 3205.00, "flow_category": "Hot Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD048", "product_name": "FOODPANDA HOT CAFE ESPANOL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 14390.00, "flow_category": "Hot Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD003", "product_name": "FOODPANDA HOT SIGNATURE BLACK COFFEE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 5585.00, "flow_category": "Hot Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD033", "product_name": "FOODPANDA HOT DARK CHOCOLATE COFFEE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 11925.00, "flow_category": "Hot Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD004", "product_name": "FOODPANDA HOT CREAMY VANILLA", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 10844.29, "flow_category": "Hot Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD051", "product_name": "FOODPANDA HOT SALTED CARAMEL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 8674.29, "flow_category": "Hot Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD104", "product_name": "FOODPANDA HOT SIGNATURE MILKY COFFEE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 6720.00, "flow_category": "Hot Coffee", "division": "NON-BREAD"},

        # Iced Coffee
        {"sku_code": "FD039", "product_name": "FOODPANDA ICED VIETNAMESE COFFEE XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 184868.57, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD100", "product_name": "FOODPANDA ICED HAZELNUT COFFEE MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 2660.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD050", "product_name": "FOODPANDA ICED CAFE ESPANOL XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 131565.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD080", "product_name": "FOODPANDA ICED SIGNATURE BLACK COFFEE MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 12176.43, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD049", "product_name": "FOODPANDA ICED CAFE ESPANOL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 108145.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD008", "product_name": "FOODPANDA ICED CREAMY VANILLA", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 107167.86, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD041", "product_name": "FOODPANDA ICED DARK CHOCOLATE COFFEE XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 106625.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD029", "product_name": "FOODPANDA ICED VIETNAMESE COFFEE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 100758.57, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD053", "product_name": "FOODPANDA ICED SALTED CARAMEL XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 99002.14, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD036", "product_name": "FOODPANDA ICED CREAMY VANILLA XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 98653.57, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD034", "product_name": "FOODPANDA ICED DARK CHOCOLATE COFFEE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 96460.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD052", "product_name": "FOODPANDA ICED SALTED CARAMEL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 72365.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD083", "product_name": "FOODPANDA ICED VIETNAMESE COFFEE MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 56307.86, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD087", "product_name": "FOODPANDA ICED CAFE ESPANOL MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 46912.14, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD084", "product_name": "FOODPANDA ICED DARK CHOCOLATE COFFEE MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 37290.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD088", "product_name": "FOODPANDA ICED SALTED CARAMEL MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 37030.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD105", "product_name": "FOODPANDA ICED SIGNATURE MILKY COFFEE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 34924.29, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD081", "product_name": "FOODPANDA ICED CREAMY VANILLA MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 30745.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD106", "product_name": "FOODPANDA ICED SIGNATURE MILKY COFFEE XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 23950.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD035", "product_name": "FOODPANDA ICED SIGNATURE BLACK COFFEE XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 22300.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD099", "product_name": "FOODPANDA ICED HAZELNUT COFFEE XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 16545.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD007", "product_name": "FOODPANDA ICED SIGNATURE BLACK COFFEE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 16373.57, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD098", "product_name": "FOODPANDA ICED HAZELNUT COFFEE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 15494.29, "flow_category": "Iced Coffee", "division": "NON-BREAD"},
        {"sku_code": "FD107", "product_name": "FOODPANDA ICED SIGNATURE MILKY COFFEE MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 14610.00, "flow_category": "Iced Coffee", "division": "NON-BREAD"},

        # Matcha
        {"sku_code": "FD059", "product_name": "FOODPANDA ICED MATCHA MILK XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 116385.71, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "FD091", "product_name": "FOODPANDA ICED VIET COFFEE MATCHA MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 12145.00, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "FD063", "product_name": "FOODPANDA ICED VIET COFFEE MATCHA", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 19190.00, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "FD060", "product_name": "FOODPANDA ICED MATCHA STRAWBERRY XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 64285.00, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "FD058", "product_name": "FOODPANDA ICED MATCHA STRAWBERRY", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 58480.00, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "FD089", "product_name": "FOODPANDA ICED MATCHA MILK MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 39824.29, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "FD064", "product_name": "FOODPANDA ICED VIET COFFEE MATCHA XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 31178.57, "flow_category": "Matcha", "division": "NON-BREAD"},
        {"sku_code": "FD090", "product_name": "FOODPANDA ICED MATCHA STRAWBERRY MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 25290.00, "flow_category": "Matcha", "division": "NON-BREAD"},

        # Choco & Milo
        {"sku_code": "FD129", "product_name": "FOODPANDA HOT MILO MOCHA", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 1915.00, "flow_category": "Choco & Milo", "division": "NON-BREAD"},
        {"sku_code": "FD114", "product_name": "FOODPANDA MILO OVRLOAD ICED XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 110261.43, "flow_category": "Choco & Milo", "division": "NON-BREAD"},
        {"sku_code": "FD005", "product_name": "FOODPANDA HOT RICH CHOCO", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 6855.00, "flow_category": "Choco & Milo", "division": "NON-BREAD"},
        {"sku_code": "FD009", "product_name": "FOODPANDA ICED RICH CHOCO", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 75698.57, "flow_category": "Choco & Milo", "division": "NON-BREAD"},
        {"sku_code": "FD037", "product_name": "FOODPANDA ICED RICH CHOCO XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 71031.43, "flow_category": "Choco & Milo", "division": "NON-BREAD"},
        {"sku_code": "FD115", "product_name": "FOODPANDA MILO OVRLOAD ICED MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 67820.00, "flow_category": "Choco & Milo", "division": "NON-BREAD"},
        {"sku_code": "FD110", "product_name": "FOODPANDA MILO MOCHA ICED XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 31425.00, "flow_category": "Choco & Milo", "division": "NON-BREAD"},
        {"sku_code": "FD082", "product_name": "FOODPANDA ICED RICH CHOCO MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 30510.00, "flow_category": "Choco & Milo", "division": "NON-BREAD"},
        {"sku_code": "FD109", "product_name": "FOODPANDA MILO MOCHA ICED", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 29915.00, "flow_category": "Choco & Milo", "division": "NON-BREAD"},
        {"sku_code": "FD111", "product_name": "FOODPANDA MILO MOCHA ICED MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 19940.00, "flow_category": "Choco & Milo", "division": "NON-BREAD"},
        {"sku_code": "FD130", "product_name": "FOODPANDA HOT MILO OVERLOAD", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 7885.00, "flow_category": "Choco & Milo", "division": "NON-BREAD"},

        # Fruity Coolers
        {"sku_code": "FD043", "product_name": "FOODPANDA FRUITY STRAWBERRY MILK", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 62059.29, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD101", "product_name": "FOODPANDA LEMON YAKULT DRINK", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 50.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD068", "product_name": "FOODPANDA LEMON BERRY COOLER XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 53665.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD047", "product_name": "FOODPANDA FRUITY MELON MILK XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 51150.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD044", "product_name": "FOODPANDA FRUITY MELON MILK", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 50465.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD046", "product_name": "FOODPANDA FRUITY STRAWBERRY MILK XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 50447.14, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD067", "product_name": "FOODPANDA LEMON BERRY COOLER", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 47222.86, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD069", "product_name": "FOODPANDA LEMON PASSION COOLER", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 31205.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD127", "product_name": "FOODPANDA COOKIES AND CREAM BLAST XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 30012.86, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD092", "product_name": "FOODPANDA LEMON BERRY COOLER MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 28934.29, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD126", "product_name": "FOODPANDA COOKIES AND CREAM BLAST", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 27610.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD070", "product_name": "FOODPANDA LEMON PASSION COOLER XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 25925.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD086", "product_name": "FOODPANDA FRUITY MELON MILK MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 23090.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD085", "product_name": "FOODPANDA FRUITY STRAWBERRY MILK MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 21170.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD128", "product_name": "FOODPANDA COOKIESAND CREAM BLAST MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 17075.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD118", "product_name": "FOODPANDA COCOPAN TWISTEA XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 16143.57, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD093", "product_name": "FOODPANDA LEMON PASSION COOLER MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 16129.29, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD119", "product_name": "FOODPANDA COCOPAN TWISTEA MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 8222.86, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD121", "product_name": "FOODPANDA MANGO SUNRISE XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 1300.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD123", "product_name": "FOODPANDA BLUEBERRY BREEZE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 1157.14, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD124", "product_name": "FOODPANDA BLUEBERRY BREEZE XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 984.29, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD120", "product_name": "FOODPANDA MANGO SUNRISE", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 765.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD122", "product_name": "FOODPANDA MANGO SUNRISE MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 420.71, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD133", "product_name": "FOODPANDA EXPRESS SWEET & CREAMY", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 180.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD131", "product_name": "FOODPANDA EXPRESS CLASSIC", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 135.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD125", "product_name": "FOODPANDA BLUEBERRY BREEZE MX", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 85.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
        {"sku_code": "FD134", "product_name": "FOODPANDA EXPRESS SWEET & CREAMY XL", "category": "NON-BREAD", "platform": "foodpanda", "gmv_q3": 80.00, "flow_category": "Fruity Coolers", "division": "NON-BREAD"},
    ]

def main():
    """Main function to populate SKU data"""
    print("🔄 CocoPan SKU Data Population Starting...")
    
    # Confirm with user
    response = input("\n⚠️  This will populate master SKU data. Continue? (y/N): ").strip().lower()
    if response != 'y' and response != 'yes':
        print("❌ Operation cancelled by user.")
        return
    
    try:
        # Get all SKUs
        grabfood_skus = get_grabfood_skus()
        foodpanda_skus = get_foodpanda_skus()
        all_skus = grabfood_skus + foodpanda_skus
        
        print(f"📊 Found {len(grabfood_skus)} GrabFood SKUs")
        print(f"📊 Found {len(foodpanda_skus)} Foodpanda SKUs")
        print(f"📊 Total: {len(all_skus)} SKUs")
        
        # Populate database
        print("\n🔄 Populating database...")
        success = db.bulk_add_master_skus(all_skus)
        
        if success:
            print("✅ SKU data population completed successfully!")
            
            # Verify population
            print("\n📋 Verification:")
            grabfood_count = len(db.get_master_skus_by_platform('grabfood'))
            foodpanda_count = len(db.get_master_skus_by_platform('foodpanda'))
            
            print(f"✅ GrabFood SKUs in database: {grabfood_count}")
            print(f"✅ Foodpanda SKUs in database: {foodpanda_count}")
            print(f"✅ Total SKUs in database: {grabfood_count + foodpanda_count}")
            
            # Show sample data
            print("\n📋 Sample GrabFood SKUs:")
            grab_sample = db.get_master_skus_by_platform('grabfood')[:5]
            for sku in grab_sample:
                print(f"  • {sku['sku_code']}: {sku['product_name']} ({sku['category']})")
            
            print("\n📋 Sample Foodpanda SKUs:")
            fp_sample = db.get_master_skus_by_platform('foodpanda')[:5]
            for sku in fp_sample:
                print(f"  • {sku['sku_code']}: {sku['product_name']} ({sku['category']})")
                
        else:
            print("❌ SKU data population failed. Check logs for details.")
            
    except Exception as e:
        logger.error(f"❌ SKU population failed: {e}")
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()