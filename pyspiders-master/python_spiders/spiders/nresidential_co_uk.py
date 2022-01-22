# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'nresidential_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source="Nresidential_Co_PySpider_united_kingdom"
    def start_requests(self):
        start_url = "https://newcomberesidential.co.uk/properties-for-rent/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'make-column-clickable-elementor')]/@data-column-clickable").getall():
            yield Request(item, callback=self.populate_item)

        next_button = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source",self.external_source)
        title=response.xpath("//title//text()").get()
        external_id=response.xpath("//p[.='Property ID: ']/parent::div/parent::div/following-sibling::div/div/p/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        if title:
            item_loader.add_value("title",title)
        rent=response.xpath("//h2[contains(.,'per month')]/text()").get()
        if rent:
            rent=rent.split("£")[1].split("per")[0].replace(",","")
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","GBP")
        adres=response.xpath("//title//text()").get()
        if adres:
            item_loader.add_value("address",adres)
        city=response.xpath("//title//text()").get()
        if city:
            item_loader.add_value("city",city.split(",")[1].strip().split(" ")[0])
            item_loader.add_value("zipcode",city.split(",")[1].strip().split("–")[0].split("  ")[-1].strip())
        property_type=response.xpath("//h1[@class='elementor-heading-title elementor-size-default']/text()").get()
        if property_type:
            item_loader.add_value("property_type",get_p_type_string(property_type))
        description="".join(response.xpath("//div[@class='elementor-text-editor elementor-clearfix']/p/text()").getall())
        if description:
            item_loader.add_value("description",description)
        room_count=response.xpath("//h2[.='Bedrooms']//parent::div/parent::div/parent::div/parent::div//h2//text()[last()]").getall()
        if room_count:
            item_loader.add_value("room_count",room_count[-1])
        bathroom_count=response.xpath("//h2[.='Bathrooms']//parent::div/parent::div/parent::div/parent::div//h2//text()[last()]").getall()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count[-1])
        images=[x for x in response.xpath("//img[@class='swiper-slide-image']//@src").getall()]
        if images:
            item_loader.add_value("images",images)
        dishwasher=response.xpath("//span[.='Dishwasher']").get()
        if dishwasher:
            item_loader.add_value("dishwasher",True)
        terrace=response.xpath("//span[.='Garden']").get()
        if terrace:
            item_loader.add_value("terrace",True)

        phone=response.xpath("//div[@class='elementor-widget-container']//a[contains(@href,'tel')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)
        item_loader.add_value("landlord_email","info@nresidential.co.uk")

       

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("house" in p_type_string.lower() or "villa" in p_type_string.lower() or "maisonette" in p_type_string.lower() or "detached" in p_type_string.lower() or "terraced" in p_type_string.lower()):
        return "house"
    else:
        return None