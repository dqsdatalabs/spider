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
    name = 'poppyresidential_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Poppyresidential_Co_PySpider_united_kingdom"

    def start_requests(self):
        yield Request("https://poppyresidentialonline.co.uk/search?type=Lettings&location=All&minBeds=1&maxBeds=10&minPrice=0&maxPrice=3000", 
                    callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='search_properties_container']/div"):
            follow_url = response.urljoin(item.xpath(".//a[contains(.,'Property Details')]/@href").get())
            property_type = " ".join(item.xpath(".//p/text()").getall())
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})

        next_button = response.xpath("//i[contains(@class,'chevron-right')]/../@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_id", response.url.split("?id=")[1])

        title = " ".join(response.xpath("//h1[contains(@class,'title')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = response.xpath("//h1[contains(@class,'title')]//text()").get()
        if address:
            if "Ha`Penny" in address and "Victoria Dock" in address:
                address = address.replace("Ha`Penny","")
                item_loader.add_value("address", address)
                item_loader.add_value("city", "Victoria Dock")
            else:
                city = address.split(",")[-1].strip()
                item_loader.add_value("address", address)
                item_loader.add_value("city", city)
        zipcode=response.xpath("//div[@id='mobile-menu']/../div/text()").get()
        if zipcode:
            zipcode=zipcode.split("[addressPostcode]")[-1].split("[")[0].replace("=> ","").strip()
            item_loader.add_value("zipcode",zipcode)
        

        rent = response.xpath("//h2[contains(@class,'rent')]//text()").get()
        if rent:
            rent = rent.strip().replace("£","").split(" ")[0].replace(",","")
            item_loader.add_value("rent", rent) 
        item_loader.add_value("currency", "GBP")

        deposit = response.xpath("//p[contains(@class,'description')]//text()[contains(.,'- Deposit:')]").get()
        if deposit:
            if "let" not in rent.lower():
                deposit = deposit.split(":")[1].strip().split(" ")[0]
                deposit = int(deposit)* int(float(int(rent)/4))
                item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//p[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        rooms = "".join(response.xpath("//p//text()[contains(.,'|')]").getall())
        if rooms:
            room_count = rooms.split("|")[0].split(":")[1].strip()
            bathroom_count = rooms.split("|")[1].split(":")[1].strip()
            item_loader.add_value("room_count", room_count)
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'property_images-slider')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//li[contains(.,'Available') or contains(.,'AVAILABLE')]//text()").getall())
        if available_date:
            if not "now" in available_date.lower():
                available_date = available_date.lower().replace("available","").replace("mid","").replace("!","").strip()
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        parking = response.xpath("//div[contains(@class,'property_features')]//li[contains(.,'Parking')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//div[contains(@class,'property_features')]//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        furnishedcheck=item_loader.get_output_value("furnished")
        if not furnishedcheck:
            furnished=response.xpath("//h4[.='Property Features']/../ul//li//text()").getall()
            if furnished:
                for i in furnished:
                    if "furnished" in i.lower():
                        item_loader.add_value("furnished",True)
 
        elevator = response.xpath("//li[contains(.,'Lift Access')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//li[contains(.,'Floor')]//text()").get()
        if floor:
            floor = floor.split("Floor")[0].replace("Luxury","").strip()
            item_loader.add_value("floor", floor.strip())

        pets_allowed = response.xpath("//li[contains(.,'Pets Allowed')]//text()[contains(.,'Yes')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)

        item_loader.add_value("landlord_name", "Poppy Residential")
        item_loader.add_value("landlord_phone", "01482 324010")
        item_loader.add_value("landlord_email", "info@poppyresidentialonline.co.uk")

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