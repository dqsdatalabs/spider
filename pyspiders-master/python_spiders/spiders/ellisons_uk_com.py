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
import dateparser
from datetime import datetime
from word2number import w2n

class MySpider(Spider):
    name = 'ellisons_uk_com'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    start_urls = ["http://www.ellisons.uk.com/api/set/results/grid"]
    form_data = {
        "page": "1",
        "sortorder": "price-desc",
        "RPP": "24",
        "OrganisationId": "b58fee63-3252-4925-96a5-da63b83718ba",
        "WebdadiSubTypeName": "Rentals",
        "Status": "{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c}",
        "includeSoldButton": "false",
    }
    
    def start_requests(self):
        yield FormRequest(self.start_urls[0], callback=self.parse, formdata=self.form_data)

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'property-wrapper')]"):
            status = item.xpath(".//div[contains(@class,'status')]").get()
            if status and "agreed" in status.lower():
                continue
            follow_url = response.urljoin(item.xpath(".//@data-url").get())
            yield Request(follow_url, callback=self.populate_item)
            seen = True
        
        if page == 2 or seen:
            self.form_data["page"] = str(page)
            yield FormRequest(self.start_urls[0], callback=self.parse, formdata=self.form_data)
        
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Ellisons_Uk_PySpider_united_kingdom")
        item_loader.add_value("external_link", response.url) 
        externalid=response.url
        if externalid:
            item_loader.add_value("external_id",externalid.split("property/")[-1].split("/")[0])
        f_text = response.url
        prop_type = ""
        if get_p_type_string(f_text):
            prop_type = get_p_type_string(f_text)
        else:
            f_text = " ".join(response.xpath("//section[@id='description']//p//text()").getall())
            if get_p_type_string(f_text):
                prop_type = get_p_type_string(f_text)
        
        if prop_type:
            item_loader.add_value("property_type", prop_type)
        else: return
        
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
            if "bedroom" in title:
                room_count = title.split("bedroom")[0].strip().split(" ")[0]
                if room_count.isdigit():
                    item_loader.add_value("room_count", room_count)
        
        address = " ".join(response.xpath("//h1/span/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
        
        city = response.xpath("//span[@class='county']/text()").get()
        if city:
            item_loader.add_value("city", city.strip().strip(","))
        citycheck=item_loader.get_output_value("city")
        if not citycheck:
            item_loader.add_value("city","London")
        
        zipcode = response.xpath("//span[@class='displayPostCode']/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        
        rent = response.xpath("//h2/span[@class='nativecurrencyvalue']/text()").get()
        if rent:
            rent = rent.replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        bathroom_count = response.xpath("//section/div/ul[@class='FeaturedProperty__list-stats']/li[2]/span/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = " ".join(response.xpath("//section[@id='description']//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        if "SQFT" in desc:
            square_meters = desc.split("SQFT")[0].strip().split("(")[-1]
            item_loader.add_value("square_meters", str(int(int(square_meters)* 0.09290304)))
        
        images = [x.split("(")[1].split(")")[0] for x in response.xpath("//div[@class='item']//@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = [x for x in response.xpath("//img[@alt='floorplan']/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
            
        deposit = response.xpath("//li[contains(.,'Deposit') or contains(.,'deposit')]//text()").get()
        if deposit:
            deposit = deposit.lower().split("week")[0].strip().split(" ")[-1]
            price = int(rent.replace(",",""))/4
            if deposit.isdigit():
                item_loader.add_value("deposit", int(float(int(deposit)*int(float(price)))))
            # else:
            #     deposit = w2n.word_to_num(deposit)
            #     item_loader.add_value("deposit", int(float(int(deposit)*int(float(price)))))
          
        floor = response.xpath("//li[contains(.,'Floor') or contains(.,'floor')]//text()").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip().capitalize()
            item_loader.add_value("floor", floor)
        
        furnished = response.xpath("//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        terrace = response.xpath("//li[contains(.,'terrace') or contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'garage') or contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(.,'balcony') or contains(.,'Balcony')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        elevator = response.xpath("//li[contains(.,'Lift') or contains(.,'lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        utilities = response.xpath("//li[contains(.,'Fee ')]//text()").get()
        if utilities:
            utilities = int(rent)/4
            item_loader.add_value("utilities", int(float(utilities)))
        lat=response.xpath("//section[@id='maps']/@data-cords").get()
        if lat:
            item_loader.add_value("latitude",lat.split("lat")[-1].split(",")[0].replace('"',"").replace(":",""))
        lon=response.xpath("//section[@id='maps']/@data-cords").get()
        if lon:
            item_loader.add_value("longitude",lon.split("lng")[-1].split("}")[0].replace('"',"").replace(":",""))
        
        item_loader.add_value("landlord_name", "Ellisons")
        item_loader.add_value("landlord_phone", "020 8944 8626")
        item_loader.add_value("landlord_email", "lettings@ellisons.uk.com")
        
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "woning" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None