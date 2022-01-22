# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.http import headers
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'kundportalen_platen'
    external_source = "KundportalenPlaten_PySpider_sweden"
    execution_type='testing'
    country='sweden' 
    locale='sv'
    start_urls = ['https://kundportalen.platen.se/rentalobject/Listapartment/published?sortOrder=NEWEST&timestamp=1631099827009']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        jsep = json.loads(data["data"])
        
        for item in jsep:
            follow_url = response.urljoin(item["DetailsUrl"])
            yield Request(follow_url, callback=self.populate_item, meta={"item": item})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        f_text = "".join(response.xpath("//div[contains(@class,'object-preview-headline')]//p//text()").getall())
        if get_p_type_string(f_text):
            item_loader.add_value("property_type", get_p_type_string(f_text))
        else:
            return
        item_loader.add_value("external_source", self.external_source)

        item = response.meta.get('item')
        item_loader.add_value("external_id", item["Id"])
        item_loader.add_value("title", item['Adress1'])
        item_loader.add_value("address", f"{item['Adress1']} {item['Adress2']} {item['Adress3']}")
        item_loader.add_value("city", item['Adress3'])
        item_loader.add_value("zipcode", item['Adress2'])
        
        item_loader.add_value("rent", item["Cost"])
        item_loader.add_value("currency", "SEK")
        
        item_loader.add_value("floor", str(item["Floor"]))
        item_loader.add_value("room_count", item["NoOfRooms"])
        item_loader.add_value("square_meters", item["Size"])
        item_loader.add_value("latitude", str(item["Latitude"]))
        item_loader.add_value("longitude", str(item["Longitude"]))
        
        import dateparser
        if item["AvailableDate"]:
            date_parsed = dateparser.parse(item["AvailableDate"], date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        if f_text:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', f_text.strip()))
        try:

            images = f"/Content/Image/{item['FirstImage']['Guid']}/0/0/True"
            item_loader.add_value("images", images)
        except:
            pass
        
        item_loader.add_value("landlord_name","Kundportalen Platen")
        item_loader.add_value("landlord_phone","0141-655 855")
        item_loader.add_value("landlord_email","info@platen.se")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "flatshare" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("lägenhet" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "hus" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "home" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    elif p_type_string and "single room" in p_type_string.lower():
        return "room"
    elif p_type_string and "bedroom" in p_type_string.lower():
        return "house"
    else:
        return None