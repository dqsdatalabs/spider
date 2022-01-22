# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request, FormRequest
from python_spiders.loaders import ListingItem
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import dateparser
import re
import json

class MySpider(Spider):
    name = "maidavaleproperties_co_uk"
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://maidavaleproperties.co.uk/lettings.php"]  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        
        for item in response.xpath("//div[@class='hover_property_h']/a[@class='info_hover_property_h']/@href").extract():
            yield Request(response.urljoin(item), self.populate_item)
            seen = True
        if page == 2 or seen:            
            p_url = f"https://maidavaleproperties.co.uk/lettings.php?pge={page}&Area=&MinBed=&TypeToSale=&PriceToSale=&PriceFromSale=&MRQ="
            yield Request(p_url, dont_filter=True, callback=self.parse, meta={"page":page+1})
  
    # 2. SCRAPING LEVEL 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        property_type = " ".join(response.xpath("//li[.='Property Type']/following-sibling::li[1]/text()").getall()).strip()
        if get_p_type_string(property_type): 
            item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return

        item_loader.add_value("external_source", "Maidavaleproperties_Co_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("id=")[1])

        title = response.xpath("//h1[contains(@class,'page-header')]//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1[contains(@class,'page-header')]//text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(",")[-1]
            if "/" in city:
                city = city.split("/")[1].strip()
            if city.replace(" ","").isalpha():
                item_loader.add_value("city", city)
            else:
                city = address.split(",")[-2]
                if "/" in city:
                    city = city.split("/")[1].strip()
                if city.replace(" ","").isalpha():
                    item_loader.add_value("city", city)

            zipcode = address.split(",")[-1]
            if not zipcode.replace(" ","").strip().isalpha():
                zipcode = zipcode
            else:
                zipcode = address.split(",")[-2]
            
            if " " not in zipcode.strip() and not zipcode.strip().isalpha():
                item_loader.add_value("zipcode", zipcode.strip())

        rent = "".join(response.xpath("//div[contains(@class,'pricing boxed')]//h2//text()").getall())
        if rent:
            rent = rent.strip().split(" ")[0].replace("£","")
            item_loader.add_value("rent", int(rent)*4)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'property-detail')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        else:
            desc = " ".join(response.xpath("//div[contains(@class,'property-detail')]//div[2]//text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(.,'Bedroom')]//following-sibling::li[1]//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]//following-sibling::li[1]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'ws_images')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'Garage') or contains(.,'PARKING')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'BALCON')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'TERRACE')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//li[contains(.,'FURNISHED')]//text()[not(contains(.,'UNFURNISHED'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'LIFT') or contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Maida Vale Properties")
        item_loader.add_value("landlord_phone", "0207 258 3737")
        item_loader.add_value("landlord_email", "info@maidavaleproperties.co.uk")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "terrace" in p_type_string.lower() or "detached" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "house"
    else:
        return None
    
