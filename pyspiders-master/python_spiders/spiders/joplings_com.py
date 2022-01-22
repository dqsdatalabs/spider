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
    name = 'joplings_com'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Joplings_PySpider_united_kingdom'
    start_urls = ['https://joplings.com/property-search/?home_comres=1&home_comtype=2&town=&type_home=&type_com=&radius=&min_beds=0&max_beds=0&min_price=&max_price=']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='featured-img']/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//div[@class='pagination']/a[contains(.,'Next')]/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//p[contains(@class,'price')]//span[contains(@class,'offer')]//text()[contains(.,'Let Agreed')]").get()     
        if status:
            return
        item_loader.add_value("external_link", response.url)
        desc = "".join(response.xpath("//section[@class='property-info']//p[1]//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else: return
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("id=")[1].split("&")[0])

        title = response.xpath("//h2//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h2//text()").get()
        if address:
            city = address.split(",")[-2]
            zipcode = address.split(",")[0]
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode)
        
        rent = response.xpath("//p[contains(@class,'price')]/text()").get()
        if rent:
            rent = rent.split("£")[1].strip().split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "AUD")

        desc = " ".join(response.xpath("//section[contains(@class,'property-info')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        from word2number import w2n
        room_count = response.xpath("//li[contains(.,'Studio')]//text()").get()
        if room_count:
                item_loader.add_value("room_count", "1")
        else:
            room_count = response.xpath("//li[contains(.,'Bedroom') or contains(.,'BEDROOM')]//text()").get()
            if room_count:
                room_count = room_count.lower().split("bedroom")[0].replace("Double","").replace("Lounge","").replace("&","").strip()
                try:
                    item_loader.add_value("room_count", w2n.word_to_num(room_count))
                except:
                    pass
            else:
                room_count = response.xpath("//h3[contains(.,'Bedroom')]//text()").get()
                if room_count:
                    room_count = room_count.split("Bedroom")[0].strip()
                    item_loader.add_value("room_count", room_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'cycle-slideshow')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//img[contains(@src,'Floorplan')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'GARAGE')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        floor = response.xpath("//li[contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.strip().split(" ")[0]
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//li[contains(.,'EPC')]//text()").get()
        if energy_label:
            energy_label = energy_label.replace("TBC","").replace("Rating","")
            if "-" in energy_label:
                energy_label = energy_label.split("-")[-1]
            else:                
                energy_label = energy_label.split("EPC")[1]
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//div[contains(@id,'map')]//iframe//@src").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('&ll=')[1].split(',')[0]
            longitude = latitude_longitude.split('&ll=')[1].split(",")[1].split('&')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        
        item_loader.add_value("landlord_name", "JOPLINGS")

        if "ripon" in address.lower():
            item_loader.add_value("landlord_phone", "01765 694800")
            item_loader.add_value("landlord_email", "ripon@joplings.com")
        elif "thirsk" in address.lower():
            item_loader.add_value("landlord_phone", "01845 522680")
            item_loader.add_value("landlord_email", "thirsk@joplings.com")

        yield item_loader.load_item()
def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "villa" in p_type_string.lower() or "detached" in p_type_string.lower() or "terrace" in p_type_string.lower() or "cottage" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None