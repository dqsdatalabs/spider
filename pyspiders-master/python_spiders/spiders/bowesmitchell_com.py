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
    name = 'bowesmitchell_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
 
    start_urls = ['https://www.bowesmitchell.com/properties-to-let']  # LEVEL 1

    # 1. FOLLOWING
    def parse(self, response):        
        for item in response.xpath("//div[@class='property-tile']"):
            status = item.xpath(".//div/img/@alt[.='Let STC']").get()
            if status:
                continue
            follow_url = response.urljoin(item.xpath(".//div[@class='eapow-property-thumb-holder']/a/@href").get())
            yield Request(follow_url, callback=self.populate_item)       

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        description = "".join(response.xpath("//div[@class='span12 eapow-desc-wrapper']/p//text()").getall())
        if get_p_type_string(description):
            item_loader.add_value("property_type", get_p_type_string(description))
        else: 
            return
        item_loader.add_value("external_source", "Bowesmitchell_PySpider_united_kingdom")

        external_id = response.xpath("//div[contains(@id,'DetailsBox')]//b[contains(.,'Ref')]//parent::div/text()").get()
        if external_id:
            external_id = external_id.replace(":","").strip()
            item_loader.add_value("external_id", external_id)

        title = " ".join(response.xpath("//div[contains(@class,'mainheader')]//h1//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        address = " ".join(response.xpath("//div[contains(@class,'mainaddress')]//text()").getall())
        if address:
            item_loader.add_value("address", address.strip())

        city_zipcode = response.xpath("//div[contains(@class,'mainaddress')]//address/text()").get()
        if city_zipcode:
            zipcode_1 = city_zipcode.strip().split(" ")[-2]
            zipcode_2 = city_zipcode.strip().split(" ")[-1]
            zipcode = zipcode_1 + " " + zipcode_2
            city = city_zipcode.split(zipcode)[0].strip()
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        rent = response.xpath("//div[contains(@class,'mainheader')]//h1//small//text()").get()
        if rent:
            rent = rent.split("£")[-1].strip().replace(",","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//div[contains(@class,'desc')]//p/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'propertyIcon-bedrooms')]//following-sibling::span//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//i[contains(@class,'propertyIcon-bathrooms')]//following-sibling::span//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'carousel')]//@data-src").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("//li[contains(.,'parking') or contains(.,'Parking') or contains(.,'Garage')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        energy_label = response.xpath("//li[contains(.,'EPC ')]//text()").get()
        if energy_label:
            energy_label = energy_label.strip().split(" ")[-1]
            item_loader.add_value("energy_label", energy_label)

        latitude_longitude = response.xpath("//script[contains(.,'lat:')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat: "')[1].split('"')[0]
            longitude = latitude_longitude.split('lon: "')[1].split('"')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@id,'DetailsBox')]//div[contains(@class,'span10')]//b//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        else:
            item_loader.add_value("landlord_name", "Bowes Mitchell Estate  Agents")

        landlord_email = response.xpath("//div[contains(@id,'DetailsBox')]//a[contains(@href,'mailto')]//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        else:
            item_loader.add_value("landlord_email", "benton@bowesmitchell.com")
        
        landlord_phone = response.xpath("//div[contains(@id,'DetailsBox')]//div[contains(@class,'phone')]/text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone.strip())
        else:
            item_loader.add_value("landlord_phone", "0191 266 4455")        

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "cottage" in p_type_string.lower() or "detached" in p_type_string.lower()):
        return "house"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    else:
        return None