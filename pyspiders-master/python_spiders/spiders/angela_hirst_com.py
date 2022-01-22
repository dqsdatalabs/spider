# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
from word2number import w2n
import json
import re

class MySpider(Spider):
    name = 'angela_hirst_com'
    execution_type='testing'
    country='united_kingdom' 
    locale='en'

    def start_requests(self):
        yield Request("https://www.angela-hirst.com/search-59", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//li[@class='propertySearchPanel']/a"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            let_available = item.xpath(".//img[contains(@alt,'Available to Let')]").get()
            if let_available: yield Request(follow_url, callback=self.populate_item)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath("//div[@class='propertyBody']//text()").get()
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Angela_Hirst_PySpider_united_kingdom")

        external_id = response.xpath("//h3[contains(.,'Reference')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            address = title.split("-")[0]
            city = address.split(",")[-1].strip()
            item_loader.add_value("title", title)
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)

        rent = response.xpath("//h2//text()").get()
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//div[contains(@class,'propertyBody')]//text()[contains(.,'Deposit')]").get()
        if deposit:
            deposit = deposit.split(":")[1].strip().split(" ")[0]
            
            number = w2n.word_to_num(deposit)
            week_rent = int(rent)/4
            deposit = int(number) * int(float(week_rent))
            item_loader.add_value("deposit", int(float(deposit)))

        desc = " ".join(response.xpath("//div[contains(@class,'propertyBody')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(.,'Bedrooms')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            try:        
                item_loader.add_value("room_count", w2n.word_to_num(room_count))
            except : pass
        
        images = [x for x in response.xpath("//div[contains(@class,'es-carousel')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        latitude_longitude = response.xpath("//iframe//@src").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('q=')[1].split(',')[0]
            longitude = latitude_longitude.split('q=')[1].split(',')[1].split('&')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Angela Hirst Chartered Surveyors")
        item_loader.add_value("landlord_phone", "01233 731177")
        item_loader.add_value("landlord_email", "hirstash@angela-hirst.com")
               
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None