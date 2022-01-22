# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'cjrealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    url = "https://www.cjrealestate.com.au/wp-admin/admin-ajax.php"
    formdata = {
        'action': 'ajax_filter_listings',
        'action_values': 'lease',
        'category_values': '',
        'city': 'All Cities',
        'order': '1',
        'newpage': '1',
        'page_id': '6247'
    }
    headers = {
        'Connection': 'keep-alive',
        'Accept': '*/*',
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://www.cjrealestate.com.au',
        'Referer': 'https://www.cjrealestate.com.au/lease-list/',
        'Accept-Language': 'tr,en;q=0.9'
    }

    def start_requests(self):
        queries = [
            {
                "query": ["apartments","unit"],
                "property_type": "apartment",
            },
            {
                "query": ["house","townhouse"],
                "property_type": "house",
            },
        ]
        for item in queries:
            for query in item.get("query"):
                self.formdata["category_values"] = query
                yield FormRequest(self.url,
                            callback=self.parse,
                            dont_filter=True,
                            formdata=self.formdata,
                            headers=self.headers,
                            meta={'property_type': item.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='property_listing']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Cjrealestate_Com_PySpider_australia")

        title = response.xpath("//title//text()").get()
        item_loader.add_value("title", title)

        address = "".join(response.xpath("//div[@class='adres_area']/text() | //div[@class='adres_area']/*[self::a]/text()").getall())
        item_loader.add_value("address", address)

        city = response.xpath("//div[@class='adres_area']/*[self::a]/text()").get()
        item_loader.add_value("city", city)

        rent = response.xpath("//b[contains(.,'Price')]/following-sibling::text()").get()
        if rent:
            rent = rent.split("$")[1].strip().replace(",","")
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "USD")
        
        features = response.xpath("//div[contains(@class,'property_location_r')]//text()").get()
        if "bed" in features:
            room_count = features.split("bed")[0].strip()
            item_loader.add_value("room_count", room_count)
        elif "studio" in features.lower():
            item_loader.add_value("room_count","1")
        if "bath" in features:
            bathroom_count = features.split("bath")[0].strip().split(" ")[-1]
            item_loader.add_value("bathroom_count", bathroom_count)
        if "car" in features:
            item_loader.add_value("parking", True)

        desc = " ".join(response.xpath("//div[@class='mobile_off_display']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "sqm" in desc:
            square_meters = desc.split("sqm")[0].strip().split(" ")[-1]
            if not "," in square_meters:
                item_loader.add_value("square_meters", square_meters)
        
        if "floor" in desc:
            floor = desc.split("floor ")[0].strip().split(" ")[-1]
            not_list = ["middle","build","timber","spac", "fantas"]
            status=True
            for i in not_list:
                if i in floor:
                    status = False
            if status:
                item_loader.add_value("floor", floor)

        furnished = response.xpath("//h2[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        parking = response.xpath("//div[contains(@class,'listing_detail')]//text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@class,'listing_detail')]//text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'listing_detail')]//text()[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//div[@class='mobile_off_display']//p//text()[contains(.,'Available')]").get()
        if available_date and "now" in available_date.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        latitude_longitude = response.xpath("//script[contains(.,'general_latitude')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('general_latitude":"')[1].split('"')[0]
            longitude = latitude_longitude.split('general_longitude":"')[1].split('"')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        images = [x for x in response.xpath("//a[contains(@class,'prettygalery')]//@src").getall()]
        if images:
            item_loader.add_value("images", images) 
        
        landlord_name = response.xpath("//div[contains(@class,'agent_details')]//h3//text()").get()
        item_loader.add_value("landlord_name", landlord_name)
        
        landlord_phone = response.xpath("//div[contains(@class,'agent_detail')]//@href[contains(.,'tel')]").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(":")[1]
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//div[contains(@class,'agent_detail')]//@href[contains(.,'mailto')]").get()
        if landlord_email:
            landlord_email = landlord_email.split(":")[1]
            item_loader.add_value("landlord_email", landlord_email)
        
     
        yield item_loader.load_item()