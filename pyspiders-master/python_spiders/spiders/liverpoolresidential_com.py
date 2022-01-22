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
    name = 'liverpoolresidential_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["http://www.liverpoolresidential.com/properties/?page=1&propind=L&country=&town=&area=&MinPrice=&MaxPrice=&MinBeds=&BedsEqual=&PropType=&Furn=&Avail=&O=PriceSearchAmount&Dir=ASC&areaId=&lat=&lng=&zoom=&searchbymap=&maplocations=&hideProps=1&location=&searchType=list"]  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'photo')]"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            status = item.xpath(".//img[contains(@src,'let')]/@alt").get()
            if not status:
                yield Request(follow_url, callback=self.populate_item)
        next_page = response.xpath("//a[contains(.,'next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))

        prop_type = response.xpath("//span[@class='bedsWithTypePropType']/text()").get()
        if get_p_type_string(prop_type):
            item_loader.add_value("property_type", get_p_type_string(prop_type))
        else: return 
                
        item_loader.add_value("external_source", "Liverpoolresidential_PySpider_united_kingdom")

        external_id = response.xpath("//div[contains(@class,'reference')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//div[contains(@class,'propertydet')]//div[contains(@class,'address')]//text()").getall())
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//div[contains(@class,'propertydet')]//div[contains(@class,'address')]//text()").get()
        if address:
            if address.count(",") == 1:
                city = address.split(",")[-1]
            else:
                zipcode = address.split(",")[-1]
                city = address.split(",")[-2]
                item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("address", address)
            item_loader.add_value("city", city.strip())

        rent = response.xpath("//div[contains(@class,'propertydet')]//div[contains(@class,'price')]//span[2]//text()").get()
        if rent:
            if "pw" in rent:
                price = response.xpath("//div[contains(@class,'propertydet')]//div[contains(@class,'price')]//span//text()").get()
                if price:
                    price = price.replace("£","").strip().replace(",","")
                    price = int(price)*4
            else:
                price = response.xpath("//div[contains(@class,'propertydet')]//div[contains(@class,'price')]//span//text()").get()
                if price:
                    price = price.replace("£","").strip().replace(",","")
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//div[contains(@class,'propertydet')]//div[contains(@class,'description')]//text()[contains(.,'Deposit:')]").get()
        if deposit:
            deposit = deposit.split(":")[1].split(".")[0].replace("£","").strip()
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//div[contains(@class,'propertydet')]//div[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(@class,'bedsWithTypeBeds')]//text()").get()
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        else:
            room_count = "".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
            if "studio" in room_count.lower():
                item_loader.add_value("room_count", "1")
                item_loader.add_value("property_type", "studio")
        
        images = [x for x in response.xpath("//div[contains(@class,'propertyimagelist')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//div[contains(@id,'floorplanlinkwrap')]//@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'propertydet')]//div[contains(@class,'description')]//text()[contains(.,'Available')]").getall())
        if available_date:
            available_date = available_date.lower().split("from")[1].strip().replace(".","")
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//div[contains(@class,'propertydet')]//div[contains(@class,'description')]//text()[contains(.,'parking')]").get()
            if parking:
                item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'BALCONY') or contains(.,'Balcon')or contains(.,'balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]//text()[not(contains(.,'Unfurnished'))]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        latitude_longitude = response.xpath("//a[contains(@data-type,'iframe')]//@href[contains(.,'lat=')]").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat=')[1].split('&')[0]
            longitude = latitude_longitude.split('lng=')[1].split('&')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Liverpool Residental")
        item_loader.add_value("landlord_phone", "0151 322 2003")
        item_loader.add_value("landlord_email", "info@liverpoolresidential.com")
        
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