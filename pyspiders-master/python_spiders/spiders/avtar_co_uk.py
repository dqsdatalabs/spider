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
    name = 'avtar_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'

    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.avtar.co.uk/property-search?feature=House&is-buy=false&show-let-agreed=true",
                    "https://www.avtar.co.uk/property-search?feature=House%20Share&is-buy=false&show-let-agreed=true",
                    "https://www.avtar.co.uk/property-search?feature=Semi-detached%20Villa&is-buy=false&show-let-agreed=true"
                    "https://www.avtar.co.uk/property-search?feature=Terraced&is-buy=false&show-let-agreed=true",
                ],
                "property_type": "house"
            },
	        {
                "url": [
                    "https://www.avtar.co.uk/property-search?feature=Flat&is-buy=false&show-let-agreed=true",
                    "https://www.avtar.co.uk/property-search?feature=Apartment&is-buy=false&show-let-agreed=true",
                ],
                "property_type": "apartment"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )
    
    # 1. FOLLOWING
    def parse(self, response):
        script = response.xpath("//search-results/@actual-properties").get()
        data = json.loads(script)
        for item in data:
            status = item["Status"]
            if status and "available" not in status.lower():
                continue
            lat, lng = item["Longitude"], item["Latitude"]
            url = f"https://www.avtar.co.uk/property-details?id={item['PropertyID']}"
            yield Request(url, callback=self.populate_item, meta={"lat":lat, "lng":lng, "item":item, "property_type": response.meta.get('property_type')})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_link", response.url)
        item = response.meta.get('item')
        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("latitude", str(response.meta["lat"]))
        item_loader.add_value("longitude", str(response.meta["lng"]))

        data = response.xpath("//property-details/@actual-property").get()
        data_j = "["+re.sub('\s{2,}', ' ', data.strip())+"]"
        jsep = json.loads(data_j)
        
        from python_spiders.helper import ItemClear
        ItemClear(response=response, item_loader=item_loader, item_name="external_source", input_value="Avtar_Co_PySpider_united_kingdom", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="address", input_value=item["Address"], input_type="VALUE")
        city = item["Address"].split(",")[-1].strip()
        ItemClear(response=response, item_loader=item_loader, item_name="city", input_value=city, input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="title", input_value="//title/text()", input_type="F_XPATH")
        ItemClear(response=response, item_loader=item_loader, item_name="description", input_value=jsep[0]["FullDescription"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="room_count", input_value=item["Bedrooms"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="bathroom_count", input_value=item["Bathrooms"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="rent", input_value=item["Price"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="currency", input_value="GBP", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="available_date", input_value=item["Available"], input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="deposit", input_value="//text()[contains(.,'Deposit')]", input_type="F_XPATH", get_num=True, split_list={"Deposit":0, " ":-1})
        for i in jsep[0]["Floorplans"]:
            item_loader.add_value("floor_plan_images", i)
        for i in jsep[0]["Thumbnails"]:
            item_loader.add_value("images", response.urljoin(i))
            
        for i in jsep[0]["Bullets"]:
            if "parking" in i.lower() or "garage" in i.lower():
                item_loader.add_value("parking", True)
                
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_name", input_value="Avtar Properties", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_phone", input_value="0113 2745111", input_type="VALUE")
        ItemClear(response=response, item_loader=item_loader, item_name="landlord_email", input_value="info@avtar.co.uk", input_type="VALUE")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "terrace" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "detached" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None