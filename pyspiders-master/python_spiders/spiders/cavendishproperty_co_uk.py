# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'cavendishproperty_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        yield Request("https://cavendishproperty.co.uk/rent/properties?page=1&pageSize=20&status=Available&publish=Published", 
                    callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='property-list']/div[contains(@class,'property')]/div"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            property_type = item.xpath("./div[contains(@class,'body')]/span[last()]/text()").get()
            if get_p_type_string(property_type): yield Request(follow_url, callback=self.populate_item, meta={"property_type": get_p_type_string(property_type)})
        
        next_button = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Cavendishproperty_Co_PySpider_united_kingdom")
        
        title = response.xpath("//title/text()").get()
        address = ""
        if title:
            item_loader.add_value("title", title)
            address = title.split("-")[0].strip()
            item_loader.add_value("address", address)

        zipcode = response.xpath("//h1/strong/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
            city = address.split(zipcode)[0].strip().strip(",")
            if not city.split(",")[-1].strip().isdigit():
                city = city.split(",")[-1].strip()
            elif not city.split(",")[-2].strip().isdigit():
                city = city.split(",")[-2].strip()
            
            item_loader.add_value("city", city.replace("NG5 1AF","").strip())

        room_count = response.xpath("//h2[contains(.,'bedroom')]/strong[1]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//span[contains(@class,'cavendish-magenta')]/text()").get()
        if rent:
            rent = rent.split(" ")[0].replace("£","").replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        
        description = " ".join(response.xpath("//div[@class='pre-line']//text()").getall())
        if description:
            description = description.replace("\r\n"," ")
            item_loader.add_value("description", description.strip())
        
        if "rating" in description.lower():
            energy_label = description.lower().split("rating")[1].replace(":","").strip().split(" ")[0]
            item_loader.add_value("energy_label", energy_label.upper())
            
        if "reference:" in description:
            external_id = description.split("reference:")[1].strip().split(" ")[0]
            if external_id.isdigit():
                item_loader.add_value("external_id", external_id)
            else:
                item_loader.add_value("external_id", response.url.split("/")[-1])
        
        if "Tenancy Deposit -" in description:
            deposit = description.split("Tenancy Deposit -")[1].split("week")[0].strip().split(" ")[-1]
            rent_pw = int(float(rent))/4
            item_loader.add_value("deposit", int(float(rent_pw))*int(deposit))

        images = [x.split("(")[1].split(")")[0] for x in response.xpath("//figure[contains(@class,'img')]/@style").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_xpath("latitude", "//div/@data-lat")
        item_loader.add_xpath("longitude", "//div/@data-long")
        
        parking = response.xpath("//div[@class='p-2'][contains(.,'parking') or contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//div[@class='p-2'][contains(.,'balcony') or contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//div[@class='p-2'][contains(.,' furnished') or contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        floor = response.xpath("//div[@class='p-2'][contains(.,'floor ') or contains(.,'Floor ')]/text()").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip().split(" ")[-1]
            item_loader.add_value("floor", floor)
        
        import dateparser
        available_date = response.xpath("//h2/span/text()").get()
        if available_date:
            available_date = available_date.split("from")[1].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        item_loader.add_value("landlord_name", "Cavendish Residential")
        item_loader.add_value("landlord_phone", "0115 941 0656")
        item_loader.add_value("landlord_email", "lettings@cavendishproperty.co.uk")
        
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