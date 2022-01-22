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
    name = 'robinsonproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        yield Request("https://www.robinsonproperty.com.au/rent/properties-for-lease/", callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'column listing')]//a[@title='Details']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-2].split("-")[-1])
        property_type = " ".join(response.xpath("//div[contains(@class,'description')]/div//text()").getall())
        if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
        else: return
        item_loader.add_value("external_source", "Robinsonproperty_Com_PySpider_australia")
        title = " ".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", title)
        item_loader.add_xpath("room_count","//p[@class='rooms text-purple']/span[i[@class='icon-bedrooms']]/text()")
        item_loader.add_xpath("bathroom_count","//p[@class='rooms text-purple']/span[i[@class='icon-bathrooms']]/text()")
        rent = response.xpath("//div[contains(@class,'price')]/div[@class='items']/text()").get()
        if rent:
            rent = "".join(filter(str.isnumeric, rent.replace(",","").split("-")[0]))
            item_loader.add_value("rent", str(int(float(rent)*4)))
        item_loader.add_value("currency", "AUD")
        address = response.xpath("//h2[contains(@class,'address')]/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(",")[-1].strip())
        zipcode = response.xpath("//script[contains(.,'postalCode')]/text()").get()
        if zipcode:
            zipcode1 = zipcode.split('"addressLocality":"')[-1].split('"')[0].strip()
            zipcode2 = zipcode.split('"postalCode":"')[-1].split('"')[0].strip()
            item_loader.add_value("zipcode", zipcode1+" "+zipcode2)
        parking = response.xpath("//p[@class='rooms text-purple']/span[i[@class='icon-carspaces']]/text()").get()
        if parking:
            if parking.strip() =="0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
       
        swimming_pool = response.xpath("//li[.='Pool']/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        furnished = response.xpath("//li[.='Furnished' or .='furnished']/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False) 
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
     
        description = "".join(response.xpath("//div[contains(@class,'description')]/div/text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        latlng = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split("L.marker([")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split("L.marker([")[1].split(",")[1].split("]")[0].strip())
        images = [response.urljoin(x) for x in response.xpath("//div[@id='gallery']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//div[@id='floorplans']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        item_loader.add_value("landlord_name", "Robinson Property")
        item_loader.add_value("landlord_phone", "0249027255")
        item_loader.add_value("landlord_email", "rentals@robinsonre.com.au")
        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "unit" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "home" in p_type_string.lower() or "bungalow" in p_type_string.lower()):
        return "house"
    else:
        return None