# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from datetime import datetime

class MySpider(Spider):
    name = 'a2dominion_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://a2dominion.co.uk//sxa/search/results/?s={5AAA3510-E39C-4E2C-9334-1995D0DF43FD}&itemid={9C543CF3-DD4E-45B1-B02B-FD6A271341B5}&sig=rent-property-result&a=London,%20UK&distance=500&tenuretype=Students,Private%20rent&g=51.5073509%7C-0.1277583&o=Distance%2CAscending&e=0&p=10&v=%7B7A3FF26C-F4BE-4A5E-A3C1-2B4D8CD26E22%7D"]

    # 1. FOLLOWING
    def parse(self, response):
        data = json.loads(response.body)
        page = response.meta.get("page", 10)

        for item in data["Results"]:
            follow_url = response.urljoin(item["Url"])
            lat, lng = item["Geospatial"]["Latitude"], item["Geospatial"]["Longitude"]
            yield Request(follow_url, callback=self.populate_item, meta={"lat":lat, "lng":lng})
        
        if page < data["Count"]:
            p_url = response.url.split("&e=")[0] + f"&e={page}" + "&p=10" + response.url.split("&p=10")[1]
            yield Request(p_url, callback=self.parse, meta={"page":page+10})

        
            
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        status = response.xpath("//div[contains(@class,'field-marketing-flag field-text')]/text()").get()
        if status and "agreed" in status.lower().strip():
            return
        
        item_loader.add_value("external_source","A2dominion_Co_PySpider_"+ self.country)
        item_loader.add_value("external_link", response.url)

        desc = "".join(response.xpath("//div[contains(@class,'tab-pane active')]//div[@class='panel-body']/p//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:
            return
        
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title)
        
        room_count = get_p_type_string(desc)
        if room_count and "studio" in room_count or "student" in room_count:
            item_loader.add_value("room_count", "1")
        elif title and "bedroom" in title:
            room_count = title.split("bedroom")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
        
        rent = "".join(response.xpath("//div[contains(@class,'section')]//li[contains(.,'£')]//text()").getall())
        price = ""
        if rent:
            if "PW" in rent:
                price = rent.split("PW")[0].split("£")[1].strip()
                item_loader.add_value("rent", str(int(float(price))*4))
            else:
                price = rent.split("PCM")[0].split("£")[1].replace(",","").strip()
                item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")
        
        address = "".join(response.xpath("//div[contains(@class,'title')]/div[@class='component-content']/p//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            zipcode = address.split(",")[-1].strip()
            city = address.split(zipcode)[0].strip().strip(",").split(",")[-1]
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode)
        

        if desc:
            item_loader.add_value("description", desc.strip())
        
        images = [ x for x in response.xpath("//div[@class='gallery-inner']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        floor_plan_images = [ response.urljoin(x) for x in response.xpath("//img[contains(@alt,'floor')]/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        item_loader.add_value("latitude", str(lat))
        item_loader.add_value("longitude", str(lng))

        energy_label = response.xpath("//div/@data-currenteff").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label_calculate(energy_label))
            
        available_date = response.xpath("//div[@class='component-content']/div[contains(@class,'marketing')]/text()").get()
        if available_date and "now" in available_date:
            available_date = datetime.now().strftime("%Y-%m-%d")
            item_loader.add_value("available_date", available_date)
        
        deposit = response.xpath("//ul/li[contains(.,'deposit')]//text()").get()
        if deposit:
            try:
                depos = deposit.split("deposit")[0].split("£")[1].strip()
                if depos.isdigit():
                    item_loader.add_value("deposit", depos)
            except:
                deposit = deposit.split("weeks")[0].strip().split(" ")[-1]
                price = int(price)//4
                item_loader.add_value("deposit", str(int(deposit)*price))
        
        unfurnished = response.xpath("//div[@class='panel-body']//li[contains(.,'unfurnished') or contains(.,'Unfurnished')]//text()").get()
        furnished = response.xpath("//div[@class='panel-body']//li[contains(.,'furnished') or contains(.,'Furnished')]//text()").get()
        if unfurnished:
            item_loader.add_value("unfurnished", False)
        elif furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//div[@class='panel-body']//li[contains(.,'Lift') or contains(.,'lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        parking = response.xpath("//div[@class='panel-body']//li[contains(.,'parking') or contains(.,'Parking') or  contains(.,'Bike storage available')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//div[@class='panel-body']//li[contains(.,'balcon') or contains(.,'Balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)

        terrace = response.xpath("//div[@class='panel-body']//li[contains(.,'terrace') or contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
            
        dishwasher = response.xpath("//div[@class='panel-body']//li[contains(.,'dishwasher') or contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        washing_machine = response.xpath("//div[@class='panel-body']//li[contains(.,'washing machine') or contains(.,'Washing machine') or contains(.,'washer')]//text()").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        item_loader.add_value("landlord_name", "A2DOMINION")
        item_loader.add_value("landlord_phone", "0800 432 0077")
        
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "house" in p_type_string.lower():
        return "house"
    else:
        return None

def energy_label_calculate(energy_number):
    energy_number = int(energy_number)
    energy_label = ""
    if energy_number <= 50:
        energy_label = "A"
    elif energy_number > 50 and energy_number <= 90:
        energy_label = "B"
    elif energy_number > 90 and energy_number <= 150:
        energy_label = "C"
    elif energy_number > 150 and energy_number <= 230:
        energy_label = "D"
    elif energy_number > 230 and energy_number <= 330:
        energy_label = "E"
    elif energy_number > 330 and energy_number <= 450:
        energy_label = "F"
    elif energy_number > 450:
        energy_label = "G"
    return energy_label