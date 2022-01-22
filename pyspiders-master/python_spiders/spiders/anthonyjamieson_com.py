# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'anthonyjamieson_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Anthonyjamieson_PySpider_united_kingdom'
    custom_settings={
        # "PROXY_ON": True,
        "HTTPCACHE_ENABLED": False,
        "CONCURRENT_REQUESTS": 4,
        "COOKIES_ENABLED": True,
        "AUTOTHROTTLE_ENABLED": False,   
        "RETRY_TIMES": 5,           
        "DOWNLOAD_DELAY": 1,
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 404, 408, 416, 456, 502, 429, 307]
    }
    
    headers={
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en,tr-TR;q=0.9,tr;q=0.8,en-US;q=0.7",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
    }
    def start_requests(self):
        yield Request(
            url="https://www.anthonyjamieson.com/wp-content/uploads/lava_all_property_1_.json",
            headers=self.headers,
            callback=self.parse,
        )

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        for item in data:
            if item["property_status"]:
                status_id = item["property_status"][0]
                if status_id:
                    if item["property_type"]:
                        prop_id = item["property_type"][0]
                        if prop_id == "68" or prop_id == "136" or prop_id == "129" or prop_id == "107":
                            property_type = "apartment"
                        elif prop_id == "115" or prop_id == "70" or prop_id == "110" or prop_id == "73" or prop_id == "113" or prop_id == "126":
                            property_type = "house"
                        elif prop_id == "101":
                            property_type = "student_apartment"
                        else:
                            property_type = ""
                            continue

                        post_id = item["post_id"]
                        lat, lng = item["lat"], item["lng"]
                        formdata = {
                            "action": "javo_map_list",
                            "post_ids[]": str(post_id),
                            "modules[map]": "module12",
                            "modules[list]": "module1",
                        }
                        yield FormRequest(
                            url="https://www.anthonyjamieson.com/wp-admin/admin-ajax.php",
                            headers=self.headers,
                            formdata=formdata,
                            callback=self.jump,
                            dont_filter=True,
                            meta={
                                "lat":lat,
                                "lng":lng,
                                "prop_type":property_type,
                            }
                        )

    def jump(self, response):
        data = json.loads(response.body)["map"]
        sel = Selector(text=data, type="html")
        for item in sel.xpath("//a[@class='three-inner-detail']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, headers=self.headers, callback=self.populate_item, meta=response.meta)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status_check = response.xpath("//i[@class='fa fa-th']/parent::div/span/text()").get()
        if status_check and ("sale" in status_check.lower() or "sold" in status_check.lower()):
            return
        
        item_loader.add_value("external_source","Anthonyjamieson_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url.split("?")[0])
        item_loader.add_value("property_type", response.meta.get('prop_type'))
        item_loader.add_value("latitude", response.meta.get('lat'))
        item_loader.add_value("longitude", response.meta.get('lng'))
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("address", "//h1[@class='uppercase']/text()")
        item_loader.add_xpath("room_count", "//div[span[.='Bedrooms']]/following-sibling::div/span/text()")
        item_loader.add_xpath("bathroom_count", "//div[span[.='Bathrooms']]/following-sibling::div/span/text()")

        rent = response.xpath("//div[p[.='Price']]/span/text()").extract_first()
        if rent:
            price = rent.replace(",","")
            item_loader.add_value("rent", price.strip())
        item_loader.add_value("currency", "GBP")
        
        address = response.xpath("//h1[@class='uppercase']/text()").extract_first()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            if address and "," in address:
                item_loader.add_value("city", address.split(",")[-2].strip())
            elif address and "." in address:
                item_loader.add_value("city", address.split(". ")[1].split(" ")[0].strip())
        
        desc = " ".join(response.xpath("//div[@id='description']//text() | //div[contains(@class,'content-wrap')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images = [x for x in response.xpath("//ul[@class='lava-attach-item slides']/li/img/@src").extract()]
        if images:
            item_loader.add_value("images", images) 

        parking = response.xpath("//div[span[.='Garages']]/following-sibling::div/span/text()").extract_first()
        if parking:
            if parking !="0":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)

        item_loader.add_value("landlord_phone", "028 9099 6122")
        item_loader.add_value("landlord_email", "hello@anthonyjamieson.com")
        item_loader.add_value("landlord_name", "Anthonyjamieson")

        
        
        
        yield item_loader.load_item()