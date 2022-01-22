
# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from word2number import w2n
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'Revereproperty_Co_PySpider_united_kingdom'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.revereproperty.co.uk/properties/rent/"] #LEVEL-1
    external_source = "Revereproperty_Co_PySpider_united_kingdom"
    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='img-container']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        next_button = response.xpath("//a[.='»']/@href").get()
        if next_button: yield Request(response.urljoin(next_button))
     
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        property_type =response.xpath("//title//text()").get()
        if get_p_type_string(property_type):
            item_loader.add_value("property_type", get_p_type_string(property_type))

        rent=response.xpath("//span[@class='rem-price-amount']/text()").get()
        if rent:
            item_loader.add_value("rent",rent.replace(",",""))
        item_loader.add_value("currency","GBP")
        item_loader.add_value("external_source", self.external_source )

        external_id = response.xpath("//strong[.='Property ID']/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        address = response.xpath("//strong[.='Address']/following-sibling::text()").get()
        if address:
            item_loader.add_value("address", address.split(":")[1].strip())
        city = response.xpath("//strong[.='City']/following-sibling::text()").get()
        if city:
            item_loader.add_value("city", city.split(":")[1].strip())
        zipcode = response.xpath("//strong[.='Postcode']/following-sibling::text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(":")[1].strip())
        title = response.xpath("//h3[@class='title fusion-responsive-typography-calculated']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        description = " ".join(response.xpath("//div[@class='description']/p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        room_count = response.xpath("//strong[.='Bedrooms']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip())
        bathroom_count = response.xpath("//strong[.='Bathrooms']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(":")[1].strip())

        img=[]
        images = [x for x in response.xpath("//section[@id='property-content']//div//div//div//div//img/@src").getall()]
        if images:
            for i in images:
                if not "data:image" in i:
                    img.append(i)
            item_loader.add_value("images", img)

        item_loader.add_value("landlord_name", "Revere")
        item_loader.add_value("landlord_email", "enquiries@revereproperty.co.uk")
      
        yield item_loader.load_item()


def get_p_type_string(p_type_string):
    if p_type_string and "student" in p_type_string.lower():
        return "student_apartment"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("apartment" in p_type_string.lower() or "flat" in p_type_string.lower() or "maisonette" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower() or "cottage" in p_type_string.lower() or "terrace" in p_type_string.lower() or "home" in p_type_string.lower()):
        return "house"
    else:
        return None
