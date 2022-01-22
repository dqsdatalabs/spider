# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'kingsfordproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en' 
    external_source="Kingsfordproperty_Com_PySpider_australia"
    def start_requests(self):
        start_url = "https://kingsfordproperty.com.au/rent/?listing_type=rent"
        yield FormRequest(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='fill-link']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        
        next_button = response.xpath("//a[@class='page-link']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)  
        title=response.xpath("//div[@class='entry-content']/h2/text()").get()
        if title:
            item_loader.add_value("title",title)
        property_type=response.xpath("//div[@class='entry-content']/h2/text()").get()
        if property_type:
            item_loader.add_value("property_type",get_p_type_string(property_type))
        rent=response.xpath("//div[@class='property-price']/text()").get()
        if rent:
            rent=rent.split("$")[1].split("/")[0]
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","USD")
        adres=response.xpath("//h1[@class='page-title']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
        room_count=response.xpath("//span[@class='tbba-icon icon-bedroom']/parent::div/preceding-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//span[@class='tbba-icon icon-bathroom']/parent::div/preceding-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//span[@class='tbba-icon icon-car']/parent::div/preceding-sibling::div/text()").get()
        if parking:
            item_loader.add_value("parking",True)
        description=title=response.xpath("//div[@class='entry-content']//div[@class='text-content']//p//text()").getall()
        if description:
            item_loader.add_value("description",description)
        images=[x for x in response.xpath("//a[@class='fill-link']//@href").getall()]
        if images:
            item_loader.add_value("images",images)
        available_date=response.xpath("//strong[.='Date available']/following-sibling::text()").get()
        if available_date:
            item_loader.add_value("available_date",available_date.strip())
        external_id=response.xpath("//strong[.='property ID']/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())
        deposit=response.xpath("//strong[.='Bond price']/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.strip())
        floor_plan_images=response.xpath("//a[@data-lightbox]/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images",floor_plan_images)
        name=response.xpath("//div[@class='property-manager-avatar']/following-sibling::p[@class='property-manager-name']/text()").get()
        if name:
            item_loader.add_value("landlord_name",name)
        email=response.xpath("//div[@class='property-manager-avatar']/following-sibling::p[@class='property-manager-email']/text()").get()
        if email:
            item_loader.add_value("landlord_email",email)
        phone=response.xpath("//div[@class='property-manager-avatar']/following-sibling::p[@class='property-manager-phone']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        
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