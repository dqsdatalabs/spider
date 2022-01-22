# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n

class MySpider(Spider):
    name = 'moreandco_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ['http://www.moreandco.co.uk/property/search_results?profile_type=lettings&min_price=&min_rent_price=&max_price=&max_rent_price=&location=&bedrooms=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
                
        for item in response.xpath("//div[@class='property_image_holder']/../div[contains(@class,'button')]//@onclick").extract():
            id = item.split("'")[1].split("&id=")[1]
            item = f"http://www.moreandco.co.uk/property/detail?id={id}"
            yield Request(item, callback=self.populate_item)
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_id", response.url.split("id=")[-1])
        item_loader.add_value("external_source", "Moreandco_Co_PySpider_united_kingdom")
        
        description = "".join(response.xpath("//h3[contains(.,'Description')]/..//p//text()").getall())
        if get_p_type_string(description): item_loader.add_value("property_type", get_p_type_string(description))
        else: return
        
        title = "".join(response.xpath("//h3[@class='primary_font_colour']/text()").getall())
        if title:
            item_loader.add_value("title", title.strip())
        
        address = "".join(response.xpath("//div[contains(@class,'main_text_colour')]/div[contains(@class,'col-md-12')]/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            add = address.strip().split(" ")
            zipcode = f"{add[-2]} {add[-1]}"
            if "Road" in zipcode:
                zipcode = zipcode.split(" ")[-1]
            item_loader.add_value("zipcode", zipcode)
            city = address.split(zipcode)[0].strip()
            if "," in city:
                city = city.split(",")[-1].strip()
            item_loader.add_value("city", city)
        
        price = response.xpath("//span[@class='guide_price']/text()").get()
        if price and "£" in price:
            rent = price.split("pcm")[0].split("£")[-1].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        if "studio" in description.lower():
            item_loader.add_value("room_count", "1")
        elif "bedroom" in description.replace("bedrrom","bedroom"):
            room_count = description.replace("bedrrom","bedroom").split("bedroom")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
            elif "single" in room_count:
                item_loader.add_value("room_count", "1")
            elif "double" in room_count:
                item_loader.add_value("room_count", "1")
            else:
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except: pass
        elif "double room" in description.lower() or "single room" in description.lower():
            item_loader.add_value("room_count", "1")
        
        if description:
            item_loader.add_value("description", description.strip())
            
        latitude_longitude = response.xpath("//iframe/@src[contains(.,'map')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split("q=")[1].split(",")[0]
            longitude = latitude_longitude.split("q=")[1].split(",")[1].split("&")[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
            
        images = [x.split("url(")[1].split(")")[0] for x in response.xpath("//div[contains(@class,'item')]//@onclick[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "More & Co")
        item_loader.add_value("landlord_phone", "0208 881 3287")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    elif p_type_string and "room" in p_type_string.lower():
        return "room"
    else:
        return None