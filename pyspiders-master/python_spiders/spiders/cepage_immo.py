# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'cepage_immo'
    execution_type='testing'
    country='france'
    locale='fr'
    custom_settings = {
        "CONCURRENT_REQUESTS" : 2,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,
    }
    headers = {
        'Content-Type': 'application/json'
    }

    def start_requests(self):
        
        payload="{\r\n    \"params\": {\r\n        \"type_offer\": \"2\",\r\n        \"prod_type\": \"house\",\r\n        \"query\": {\r\n            \"prod.prod_type\": \"house\"\r\n        }\r\n    },\r\n    \"hash\": \"eyJ0eXBlX29mZmVyIjoiMiIsInByb2RfdHlwZSI6ImhvdXNlIn0=\",\r\n    \"cached\": {\r\n        \"prodId\": [\r\n            \"VM14692\",\r\n            \"VM14758\",\r\n            \"VA7888\",\r\n            \"VM14387\",\r\n            \"VM14694\"\r\n        ],\r\n        \"prodResults\": [\r\n            \"790d57da-37c9-4091-ab80-dbeaf005bf02\"\r\n        ],\r\n        \"agenceId\": [\r\n            \"2\",\r\n            \"default\"\r\n        ],\r\n        \"userId\": [\r\n            \"1\",\r\n            \"104\",\r\n            \"106\",\r\n            \"108\",\r\n            \"110\",\r\n            \"112\",\r\n            \"114\"\r\n        ],\r\n        \"type\": [\r\n            \"activites\",\r\n            \"geoloc\",\r\n            \"promo\",\r\n            \"park\",\r\n            \"type_comm\",\r\n            \"type_offer\",\r\n            \"type_park\",\r\n            \"type_pro\",\r\n            \"type_viager\",\r\n            \"prog_defisc\",\r\n            \"type_land\",\r\n            \"price_pro_taxe\",\r\n            \"prog_livraison\",\r\n            \"prod_type\"\r\n        ],\r\n        \"lang\": [\r\n            \"multiTextFirst\",\r\n            \"search.btn\",\r\n            \"type_offer\",\r\n            \"prod_type\",\r\n            \"activites\",\r\n            \"geo\",\r\n            \"month\",\r\n            \"search.btnOpen\",\r\n            \"formProdTextFirst\",\r\n            \"formProdTextGeo\",\r\n            \"formProdTextBudget\",\r\n            \"formProdTextAnd\",\r\n            \"formProdTextWith\",\r\n            \"formProdTextSurface\",\r\n            \"filter.region\",\r\n            \"filter.dpt\",\r\n            \"filter.displayMap\",\r\n            \"filter.regionOption0\",\r\n            \"filter.regionOption1\",\r\n            \"cardContact.btnEmail\",\r\n            \"cardContact.negoDetail\",\r\n            \"cardContact.around\",\r\n            \"cardContact.goMap\",\r\n            \"cardContact.agDetail\",\r\n            \"cardContact.btnTel\",\r\n            \"valuation.btnEstimation\",\r\n            \"filter.displayTypeOptions1\",\r\n            \"filter.displayTypeOptions2\",\r\n            \"filter.orderByOptions1\",\r\n            \"filter.orderByOptions2\",\r\n            \"filter.orderByOptions3\",\r\n            \"filter.orderByOptions4\",\r\n            \"filter.orderByOptions5\",\r\n            \"filter.orderByOptions6\",\r\n            \"social.follow\",\r\n            \"contact.title\",\r\n            \"contact.subtitle\",\r\n            \"contact.btn\",\r\n            \"contact.tel\",\r\n            \"contact.email\",\r\n            \"contact.firstname\",\r\n            \"contact.lastname\",\r\n            \"contact.rgpd\",\r\n            \"contact.message\",\r\n            \"contact.confirm\",\r\n            \"footer.popularTitle\",\r\n            \"footer.propTitle\",\r\n            \"footer.lastPublishedTitle\",\r\n            \"footer.otherLinksTitle\",\r\n            \"footer.copyrights_prefix\",\r\n            \"menu.label\",\r\n            \"formPropConnect.title\",\r\n            \"formPropConnect.btn\",\r\n            \"contact.password\"\r\n        ],\r\n        \"prodUrl\": [\r\n            \"lastPublished\",\r\n            \"populars\"\r\n        ],\r\n        \"metas\": [\r\n            \"title\",\r\n            \"description\",\r\n            \"image\",\r\n            \"lastModified\"\r\n        ]\r\n    },\r\n    \"username\": \"cepage\",\r\n    \"pageType\": \"ProductsList\",\r\n    \"lang\": \"fr\",\r\n    \"userAgent\": \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36\",\r\n    \"location\": \"/location/maison?prod.prod_type=house\"\r\n}"
        p_url = "https://www.cepage.immo/webapi/getJson/Templates/ProductsList"
        yield Request(
            p_url,
            callback=self.parse,
            body=payload,
            headers=self.headers,
            method="POST",
            meta={
                "p_type":"house"
            }
        )

        payload1="{\r\n    \"params\": {\r\n        \"type_offer\": \"2\",\r\n        \"prod_type\": \"appt\",\r\n        \"query\": {\r\n            \"prod.prod_type\": \"appt\"\r\n        }\r\n    },\r\n    \"hash\": \"eyJ0eXBlX29mZmVyIjoiMiIsInByb2RfdHlwZSI6ImFwcHQifQ==\",\r\n    \"cached\": {\r\n        \"prodId\": [\r\n            \"LM14551\",\r\n            \"LM14611\"\r\n        ],\r\n        \"prodResults\": [\r\n            \"790d57da-37c9-4091-ab80-dbeaf005bf02\",\r\n            \"8a1d3d1a-3a28-49cf-8783-df4f72fac14c\",\r\n            \"search\"\r\n        ],\r\n        \"prodCount\": [],\r\n        \"agenceId\": [\r\n            \"2\",\r\n            \"default\"\r\n        ],\r\n        \"userId\": [\r\n            \"1\",\r\n            \"104\",\r\n            \"106\",\r\n            \"108\",\r\n            \"110\",\r\n            \"112\",\r\n            \"114\"\r\n        ],\r\n        \"type\": [\r\n            \"activites\",\r\n            \"geoloc\",\r\n            \"promo\",\r\n            \"park\",\r\n            \"type_comm\",\r\n            \"type_offer\",\r\n            \"type_park\",\r\n            \"type_pro\",\r\n            \"type_viager\",\r\n            \"prog_defisc\",\r\n            \"type_land\",\r\n            \"price_pro_taxe\",\r\n            \"prog_livraison\",\r\n            \"prod_type\"\r\n        ],\r\n        \"lang\": [\r\n            \"multiTextFirst\",\r\n            \"search.btn\",\r\n            \"type_offer\",\r\n            \"prod_type\",\r\n            \"activites\",\r\n            \"geo\",\r\n            \"month\",\r\n            \"search.btnOpen\",\r\n            \"formProdTextFirst\",\r\n            \"formProdTextGeo\",\r\n            \"formProdTextBudget\",\r\n            \"formProdTextAnd\",\r\n            \"formProdTextWith\",\r\n            \"formProdTextSurface\",\r\n            \"filter.region\",\r\n            \"filter.dpt\",\r\n            \"filter.displayMap\",\r\n            \"filter.regionOption0\",\r\n            \"filter.regionOption1\",\r\n            \"cardContact.btnEmail\",\r\n            \"cardContact.negoDetail\",\r\n            \"cardContact.around\",\r\n            \"cardContact.goMap\",\r\n            \"cardContact.agDetail\",\r\n            \"cardContact.btnTel\",\r\n            \"valuation.btnEstimation\",\r\n            \"filter.displayTypeOptions1\",\r\n            \"filter.displayTypeOptions2\",\r\n            \"filter.orderByOptions1\",\r\n            \"filter.orderByOptions2\",\r\n            \"filter.orderByOptions3\",\r\n            \"filter.orderByOptions4\",\r\n            \"filter.orderByOptions5\",\r\n            \"filter.orderByOptions6\",\r\n            \"social.follow\",\r\n            \"contact.title\",\r\n            \"contact.subtitle\",\r\n            \"contact.btn\",\r\n            \"contact.tel\",\r\n            \"contact.email\",\r\n            \"contact.firstname\",\r\n            \"contact.lastname\",\r\n            \"contact.rgpd\",\r\n            \"contact.message\",\r\n            \"contact.confirm\",\r\n            \"footer.popularTitle\",\r\n            \"footer.propTitle\",\r\n            \"footer.lastPublishedTitle\",\r\n            \"footer.otherLinksTitle\",\r\n            \"footer.copyrights_prefix\",\r\n            \"menu.label\",\r\n            \"formPropConnect.title\",\r\n            \"formPropConnect.btn\",\r\n            \"contact.password\",\r\n            \"formAlert.title\",\r\n            \"formAlert.subtitle\",\r\n            \"formAlert.btn\",\r\n            \"formAlert.confirm\"\r\n        ],\r\n        \"prodUrl\": [\r\n            \"lastPublished\",\r\n            \"populars\"\r\n        ],\r\n        \"metas\": [\r\n            \"title\",\r\n            \"description\",\r\n            \"image\",\r\n            \"lastModified\"\r\n        ]\r\n    },\r\n    \"username\": \"cepage\",\r\n    \"pageType\": \"ProductsList\",\r\n    \"lang\": \"fr\",\r\n    \"userAgent\": \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36\",\r\n    \"location\": \"/location/appartement?prod.prod_type=appt\"\r\n}"
        p_url = "https://www.cepage.immo/webapi/getJson/Templates/ProductsList"
        yield Request(
            p_url,
            callback=self.parse,
            body=payload1,
            headers=self.headers,
            method="POST",
            meta={
                "p_type":"apartment"
            }
        )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for i in data["data"]["prodId"].keys():
            for item in data["data"]["prodId"][i].keys():
                follow_url = f"https://www.cepage.immo/location/{data['data']['prodId'][i]['url']},{data['data']['prodId'][i]['prod_ref']}"
                yield Request(follow_url, callback=self.populate_item, meta={"p_type":response.meta["p_type"]})
        

     # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get("p_type"))

        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Cepage_PySpider_france", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="external_id", input_value="//span[contains(.,'Référence')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value="//span[contains(.,'Localisation')]/following-sibling::span/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value="//script[contains(.,'\"@type\":\"Product\"')]/text()", input_type="F_XPATH", split_list={'"addressLocality":"':-1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="zipcode", input_value="//script[contains(.,'\"@type\":\"Product\"')]/text()", input_type="F_XPATH", split_list={'"postalCode":"':-1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value="//span[contains(.,'Descriptif')]/../../../../../../div[3]//text()", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="square_meters", input_value="//span[contains(.,'Surface')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value="//span[contains(.,'Chambres')]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value="//span[contains(.,\"Salle d'eau\")]/following-sibling::span/text()", input_type="F_XPATH", get_num=True)
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value="//span[@class='prefix' and contains(.,'mois')]/preceding-sibling::text()", input_type="F_XPATH", get_num=True, split_list={".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="EUR", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value="//p[contains(.,'disponible ')]/text()", input_type="F_XPATH", split_list={"le":-1, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//span[contains(@class,'textblock') and contains(.,'Dépôt de garantie')]/text()", input_type="F_XPATH", get_num=True, split_list={"Dépôt de garantie":1, "€":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="images", input_value="//div[@class='_e29fm8 _3hmsj _1rm8tvl theme1']//img/@data-src", input_type="M_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="latitude", input_value="//script[contains(.,'latitude')]/text()", input_type="F_XPATH", split_list={'"latitude":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="longitude", input_value="//script[contains(.,'latitude')]/text()", input_type="F_XPATH", split_list={'"longitude":"':1, '"':0})
        ItemClear(response=response, item_loader=item_loader, item_name="utilities", input_value="//span[contains(@class,'textblock') and contains(.,'Provision sur charges')]/text()", input_type="F_XPATH", get_num=True, split_list={"Provision sur charges":1, "€":0, ".":0})
        ItemClear(response=response, item_loader=item_loader, item_name="swimming_pool", input_value="//span[contains(.,'Piscine')]/following-sibling::span/text()", input_type="F_XPATH", tf_item=True)
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Cepage", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="04 68 64 10 67", input_type="VALUE")

        furnished = response.xpath("//span[contains(.,'Ameublement')]/following-sibling::span/text()").get()
        if furnished: 
            if 'Non meubl' in furnished: item_loader.add_value("furnished", False)
            elif 'meubl' in furnished: item_loader.add_value("furnished", True)

        yield item_loader.load_item()
