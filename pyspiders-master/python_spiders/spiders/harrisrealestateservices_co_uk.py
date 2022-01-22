# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'harrisrealestateservices_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    start_urls = ['https://www.harrisrealestateservices.co.uk/properties/properties-to-let-leeds']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//li[@class='featured']"):
            url = item.xpath(".//p[@class='propertylistinglinks']/span/a/@href").get()
            room_count = item.xpath(".//li[contains(.,'Bed')]//text()").get()
            bathroom_count = item.xpath(".//li[contains(.,'Bath')]//text()").get()
            status = item.xpath(".//span[@class='type_name']/text()[contains(.,'LET AGREED')]").get()
            if status:
                continue
            yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta.get('property_type'), "room_count":room_count,"bathroom_count":bathroom_count})
        
        next_page = response.xpath("//ul[@class='pagination']/li/a[@title='Next']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
    
        desc = "".join(response.xpath("//div[@id='desctab']//div//text()").getall())
        if get_p_type_string(desc):
            item_loader.add_value("property_type", get_p_type_string(desc))
        else:             
            return
        item_loader.add_value("external_source", "Harrisrealestateservices_Co_PySpider_united_kingdom")

        title = response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = "".join(response.xpath("//span[contains(@class,'location')]//parent::div/text()").getall()).strip()
        if address:
            city = address.split(",")[-1].strip()
            if city.replace(" ","").isalpha():
                item_loader.add_value("city", city)

            zipcode = address.split(",")[-1].strip()
            if not zipcode.replace(" ","").isalpha():
                item_loader.add_value("zipcode", zipcode)
            elif not address.split(",")[-2].strip().replace(" ","").isalpha():
                zipcode = address.split(",")[-2].strip()
                item_loader.add_value("zipcode", zipcode)
            elif not address.split(",")[-3].strip().replace(" ","").isalpha():
                zipcode = address.split(",")[-3].strip()
                item_loader.add_value("zipcode", zipcode)

            item_loader.add_value("address", address.strip())
        
        addresscheck=item_loader.get_output_value("address")
        if not addresscheck:
            adres=response.xpath("//title/text()").get()
            if adres:
                item_loader.add_value("address",adres.split("!")[-1])
                item_loader.add_value("city",adres.split("!")[-1])


        rent = "".join(response.xpath("//span[contains(@class,'price')]//text()").getall())
        if rent:
            if "." in rent:
                rent = rent.split("£")[1].split(".")[0].strip().replace(",","").replace("\u00a0","")
                item_loader.add_value("rent", rent)
            else:
                if "£" in rent:
                    rent = rent.split("£")[1].strip().split(" ")[0]
                    item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@id,'desctab')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.meta.get("room_count")
        if room_count:
            room_count = room_count.strip().split(" ")[0]
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.meta.get("bathroom_count")
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//a[contains(@class,'propertyphotogroup')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//div[contains(@id,'desctab')]//text()[contains(.,'parking') or contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//div[contains(@id,'desctab')]//text()[contains(.,'balcony') or contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = "".join(response.xpath("//div[contains(@id,'desctab')]//text()[contains(.,'Furnished')]").getall())
        if furnished:
            item_loader.add_value("furnished", True)

        energy_label = response.xpath("//div[contains(@id,'desctab')]//text()[contains(.,'EPC')]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip().split(" ")[-1])

        latitude_longitude = response.xpath("//script[contains(.,'LatLng(')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0]
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            if not(latitude == "0" or longitude == "0"):
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "HARRIS REAL ESTATE SERVICES")
        item_loader.add_value("landlord_phone", "0113 2555 208")
        item_loader.add_value("landlord_email", "info@harrisrealestateservices.co.uk")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and ("villa" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None