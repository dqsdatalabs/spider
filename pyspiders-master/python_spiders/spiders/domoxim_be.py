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
import unicodedata
import re

class MySpider(Spider):
    name = 'domoxim_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    }
    handle_httpstatus_list = [404]

    def start_requests(self):
        url = "https://www.domoxim.be/page-data/nl/te-huur/page-data.json"
        headers = {
            'authority': 'www.domoxim.be',
            'origin': 'https://www.domoxim.be',
            'accept': '*/*',
            'referer': 'https://www.domoxim.be/nl/te-huur/',
            'accept-language': 'tr,en;q=0.9,en-GB;q=0.8,en-US;q=0.7'
        }
        yield Request(url, headers=headers, callback=self.parse)

    # 1. FOLLOWING 
    def parse(self, response):
        
        data = json.loads(response.body)
        for item in data["result"]["pageContext"]["data"]["contentRow"][0]["data"]["propertiesList"]:
            city = item["City"].lower().strip().replace(" ", "").replace("-", "")
            description = slugify(item["TypeDescription"])
            id_ = str(item["ID"])
            url = f"https://www.domoxim.be/nl/te-huur/{city}/{description}/{id_}/"
            yield Request(url, callback=self.populate_item, meta={"item":item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
            
        property_type = response.xpath("//title/text()").get()
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return

        item_loader.add_value("external_source", "Domoxim_PySpider_belgium")
        item_loader.add_value("external_link", response.url)
        
        item = response.meta.get('item')
        
        title = item["TypeDescription"]
        item_loader.add_value("title", title)
        
        item_loader.add_value("external_id", str(item["ID"]))
        if str(item["ID"]) == "1867799":
            print(item)
        
        street = item["Street"]
        city = item["City"]
        item_loader.add_value("address", f"{street} {city}")
        item_loader.add_value("city", city)
        zipcode = item["Zip"]
        item_loader.add_value("zipcode", zipcode)
        
        item_loader.add_value("latitude", str(item["GoogleX"]))
        item_loader.add_value("longitude", str(item["GoogleY"]))
        
        item_loader.add_value("description", item["DescriptionA"])
        item_loader.add_value("square_meters", item["SurfaceTotal"])
        if item["NumberOfBedRooms"] !='0':
            item_loader.add_value("room_count", item["NumberOfBedRooms"])
        item_loader.add_value("bathroom_count", item["NumberOfBathRooms"])
        item_loader.add_value("rent", item["Price"])
        item_loader.add_value("currency", "EUR")
        utilities=response.xpath("//script[contains(.,'postal')]/text()").get()
        if utilities:
            uti=json.loads(utilities)
            utii=uti['priceSpecification']['monthlyExtraCosts']
            item_loader.add_value("utilities",utii)
        for i in item["LargePictures"]:
            item_loader.add_value("images", i)
        
        parking = item["NumberOfGarages"]
        if parking !=0:
            item_loader.add_value("parking", True)
        
        try:
            item_loader.add_value("energy_label", item["EnergyPerformance"])
        except: pass
        
        if " gemeubelde" in title.lower():
            item_loader.add_value("furnished", True)
            
        item_loader.add_value("landlord_name","DOMOXIM")
        item_loader.add_value("landlord_phone","0524 976 371")
        item_loader.add_value("landlord_email","info@domoxim.com")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "appartement" in p_type_string.lower() or "bovenwoning" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None
    
def slugify(title_string):
    value = unicodedata.normalize('NFKD', title_string).encode('ascii', 'ignore').decode('ascii')
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    value = re.sub('[-\s]+', '-', value)
    return value