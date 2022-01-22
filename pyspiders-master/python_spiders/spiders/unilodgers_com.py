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
    name = 'unilodgers_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    custom_settings = {
        "PROXY_ON": True,
        "RETRY_HTTP_CODES": [500, 503, 504, 400, 401, 403, 405, 407, 408, 416, 456, 502, 429, 307]
    }
    headers = {
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36'
    }

    def start_requests(self):
        start_urls = [
            {"url": "https://www.unilodgers.com/sitemap/uk"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            headers=self.headers,
                            callback=self.jump)

    def jump(self, response):
        for item in response.xpath("//div[contains(@id,'countries')]//li//a//@href").extract():
            yield Request(response.urljoin(item), callback=self.parse, headers=self.headers)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        seen=False
        for item in response.xpath("//div[contains(@class,'property-list-container')]//h5//parent::a//@href").extract():
            follow_url = response.urljoin(item)
            slug = item.split("/")[-1]
            f_url= f"https://www.unilodgers.com/api/v1/properties/{slug}/details"
            yield Request(f_url, callback=self.populate_item, meta={'external_link': follow_url})
            seen=True
        
        if page ==2 or seen:        
            f_url = response.url.replace(f"page={page-1}", f"page={page}")
            yield Request(f_url, callback=self.parse, headers=self.headers, meta={"page": page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", "room")
        item_loader.add_value("external_link", response.meta.get('external_link'))
        item_loader.add_value("external_source", "Unilodgers_PySpider_united_kingdom")
        data = json.loads(response.body)
        item = data['data']
        external_id = item['id']
        if external_id:
            item_loader.add_value("external_id", external_id)
        address = item['attributes']['address']
        if address:
            item_loader.add_value("address", address)
        latitude = item['attributes']['latitude']
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = item['attributes']['longitude']
        if longitude:
            item_loader.add_value("longitude", longitude)

        rent = item['attributes']['price_from']
        if rent:
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "GBP")
        
        item_loader.add_value("room_count", "1")
        
        title = item['attributes']['pname']
        if title:
            item_loader.add_value("title", title)

        desc = item['attributes']['description']
        if desc:
            item_loader.add_value("description", desc)
        images = data['included']
        for x in images:
            if x['type'] == "property_image":
                item_loader.add_value("images", f"https://assets2.unilodgers.com/cdn-cgi/image/width=1032.5,height=558.75,quality=80,format=auto{x['attributes']['p_image_url']}")

        item_loader.add_value("landlord_name", "UNILODGERS")
        item_loader.add_value("landlord_phone", "+44-203-8089266")
        item_loader.add_value("landlord_email", "hello@unilodgers.com")

        if item['attributes']["fully_booked"]:
            return

        address = item['attributes']['address']

        zipcode = re.search("[A-Z\d]{2,3} [A-Z\d]{3}",address)
        if zipcode:
            zipcode = zipcode[0]
            item_loader.add_value("zipcode",zipcode)

        city = response.meta.get('external_link').split("uk/")[-1].split("/")[0]
        if city:
            item_loader.add_value("city",city)

        if "reading" in response.meta.get('external_link'):
            return


        
        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "local" in p_type_string.lower():
        return None
    elif p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("studio" in p_type_string.lower() or "t1" in p_type_string.lower()):
        return "studio"
    elif p_type_string and ("appartement" in p_type_string.lower() or "f1" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "t2" in p_type_string.lower() or "t3" in p_type_string.lower() or "t4" in p_type_string.lower() or "t5" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("maison" in p_type_string.lower() or "villa" in p_type_string.lower()):
        return "house"
    elif p_type_string and "chambre" in p_type_string.lower():
        return "room"   
    else:
        return None