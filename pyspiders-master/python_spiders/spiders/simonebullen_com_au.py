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
    name = 'simonebullen_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7',
        'Host': 'simonebullen.com.au',
        'Proxy-Connection': 'keep-alive',
        'Referer': 'http://simonebullen.com.au/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.128 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    start_url = "http://simonebullen.com.au/Home/Properties&start=0"

    def start_requests(self): # LEVEL 1
        yield Request(url=self.start_url,
                        headers=self.headers,
                        callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 8)
        seen=False
        for item in response.xpath("//div[contains(@class,'property')]"):
            item_loader = ListingLoader(response=response)
            item_loader.add_value("external_link", "http://simonebullen.com.au/")
            item_loader.add_value("external_source", "Simonebullen_PySpider_australia")
            property_type = item.xpath(".//div[contains(@class,'features')]//text()[contains(.,'Property Type')]").get()
            # else: return

            title = item.xpath(".//span[contains(@class,'tagline')]//text()").get()
            if title:
                item_loader.add_value("title", title)
            else:
                title = item.xpath(".//span[contains(@class,'name')]//text()").get()
                if title:
                    item_loader.add_value("title", title)

            rent = item.xpath(".//span[contains(@class,'price')]//text()").get()
            if rent:
                rent = rent.split("/")[0].replace("$","").strip()
                rent = int(rent)*4
                item_loader.add_value("rent", rent)
            item_loader.add_value("currency", "AUD")

            address = item.xpath(".//span[contains(@class,'name')]//text()").get()
            if address:
                city = address.split(",")[-1].strip()
                item_loader.add_value("address", address)
                item_loader.add_value("city", city)

            room_count = item.xpath(".//span[contains(@class,'bedroom')]//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

            bathroom_count = item.xpath(".//span[contains(@class,'bathroom')]//text()").get()
            if bathroom_count:
                item_loader.add_value("bathroom_count", bathroom_count.strip())

            parking = item.xpath(".//span[contains(@class,'parking')]//text()").get()
            if parking:
                item_loader.add_value("parking", True)

            desc = " ".join(response.xpath("//span[contains(@class,'tagline')]//following-sibling::p//text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)

            images = [x.split("url('")[1].split("'")[0] for x in item.xpath(".//div[contains(@class,'property-gallery')]//@style[contains(.,'background')]").getall()]
            if images:
                item_loader.add_value("images", images)

            latitude = item.xpath(".//div[contains(@id,'property-map')]//@data-lat").get()
            if latitude and int(float(latitude)) != 0:
                item_loader.add_value("latitude", latitude)

            longitude = item.xpath(".//div[contains(@id,'property-map')]//@data-lng").get()
            if longitude and int(float(longitude)) != 0:
                item_loader.add_value("longitude", longitude)

            item_loader.add_value("landlord_name", "SIMONE BULLEN")
            item_loader.add_value("landlord_phone", "(03) 9370 0246")
            item_loader.add_value("landlord_email", "enquiries@simonebullen.com.au")
            if get_p_type_string(property_type):
                item_loader.add_value("property_type", get_p_type_string(property_type))
                yield item_loader.load_item()

            seen=True
        
        if page ==8 or seen:      
            f_url = response.url.replace(f"start={page-8}", f"start={page}")
            yield Request(f_url, headers=self.headers, callback=self.parse, meta={"page": page+8})

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house" 
    else:
        return None