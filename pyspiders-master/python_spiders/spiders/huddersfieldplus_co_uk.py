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
    name = 'huddersfieldplus_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'
    def start_requests(self):
        yield Request("https://www.huddersfieldplus.co.uk/properties-to-let", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(.,'Read more')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type = " ".join(response.xpath("//div[@id='propdescription']//text()").getall()).strip()
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        item_loader.add_value("external_source", "Huddersfieldplus_Co_PySpider_united_kingdom")
        title = response.xpath("//h1/text()[normalize-space()]").get()
        if title:
            item_loader.add_value("title", title.strip())
        address = ", ".join(response.xpath("//div[contains(@class,'eapow-mainaddress')]//text()").getall())
        if address:
            item_loader.add_value("address", re.sub('\s{2,}', ' ', address.strip()))
        item_loader.add_xpath("bathroom_count", "//div/img[@alt='Bathrooms']/following-sibling::strong[1]/text()")
        item_loader.add_xpath("room_count", "//div/img[@alt='Bedrooms']/following-sibling::strong[1]/text()")
        item_loader.add_xpath("deposit", "//li[contains(.,'Deposit ')]/text()")   
        rent_string =  response.xpath("//h1/small[@class='eapow-detail-price']/text()").get()
        if rent_string:
            if "week" in rent_string.lower():
                rent = rent_string.split("£")[-1].split(" ")[0].replace(",","")
                item_loader.add_value("rent", str(int(rent.strip())*4))
                item_loader.add_value("currency", "GBP")
            else:
                item_loader.add_value("rent_string", rent_string)
        description = " ".join(response.xpath("//div[contains(@class,'eapow-desc-wrapper')]/p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        city = response.xpath("//div[b='County']/text()").get()
        if city:
            item_loader.add_value("city", city.replace(":","").strip())
        zipcode = response.xpath("//div[contains(@class,'eapow-mainaddress')]/address/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", " ".join(zipcode.strip().split(" ")[-2:]))
            
        external_id = response.xpath("//b[contains(.,'Ref')]/following-sibling::text()").get()
        if external_id:
            external_id = external_id.replace(":","").strip()
            item_loader.add_value("external_id", external_id)
        
        script_map = response.xpath("//script/text()[contains(.,'lat:')]").get()
        if script_map:
            item_loader.add_value("latitude", script_map.split('lat: "')[1].split('"')[0].strip())
            item_loader.add_value("longitude", script_map.split('lon: "')[1].split('"')[0].strip())

        parking = response.xpath("//li[contains(.,'Parking') or contains(.,'Garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        furnished = response.xpath("//li[contains(.,'Furnished') or contains(.,'furnished')]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower() or "un-furnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
         
        images = [x for x in response.xpath("//div[@id='eapowgalleryplug']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_xpath("landlord_name", "//div[@id='DetailsBox']//a/b/text()")
        item_loader.add_xpath("landlord_phone", "//div[@id='DetailsBox']//div[contains(@class,'sidecol-phone')]/text()")
        item_loader.add_value("landlord_email", "office@huddersfieldlettings.co.uk")

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "bedroom" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "terrace" in p_type_string.lower()):
        return "house"
    else:
        return None