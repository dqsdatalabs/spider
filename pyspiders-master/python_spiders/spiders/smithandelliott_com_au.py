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
    name = 'smithandelliott_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source = 'Smithandelliott_Com_PySpider_australia'
    def start_requests(self):
        start_url = "https://www.smithandelliott.com.au/for-rent/"
        yield Request(start_url, callback=self.parse)

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@id,'listing-')]//h2/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item)
        
        next_button = response.xpath("//a[@class='next page-numbers']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//span[@class='listing-price-value']//text()[contains(.,'APPROVED')]").get()
        if status:
            return
        item_loader.add_value("external_source", self.external_source)          
        item_loader.add_xpath("title","//title/text()")
        property_type = "".join(response.xpath("//div[@class='wpsight-listing-description']//text()").getall())
        if property_type:
            if get_p_type_string(property_type): item_loader.add_value("property_type", get_p_type_string(property_type))
            else: return
        item_loader.add_value("external_link", response.url)

        rent = "".join(response.xpath("//span[@class='listing-price-value']/text()[.!='APPROVED APPLICATION!']").getall())
        if rent:
            if "$" in rent:
                price = rent.split(" ")[0].replace("$","").strip()
                item_loader.add_value("rent", int(float(price)) * 4)
        item_loader.add_value("currency", 'AUD')


        external_id = "".join(response.xpath("//div[@class='alignright']/div/text()").getall())
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        item_loader.add_xpath("room_count", "normalize-space(//span[@title='Beds']/span[@class='listing-details-value']/text())")
        item_loader.add_xpath("bathroom_count","normalize-space(//span[@title='Baths']/span[@class='listing-details-value']/text())")

        address = "".join(response.xpath("//h3[@class='address']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split(" ")[-1].strip())
            item_loader.add_value("city", address.split(",")[1].strip().split(" ")[0])

        item_loader.add_xpath("latitude", "substring-before(substring-after(//script/text()[contains(.,'LatLng(')],'LatLng('),',')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(substring-after(//script/text()[contains(.,'LatLng(')],'LatLng('),','),')')")

        description = " ".join(response.xpath("//div[@itemprop='description']/p/text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@class='wpsight-image-slider-item']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)

        parking = response.xpath("normalize-space(//span[@title='Car Park']/span[@class='listing-details-value']/text())").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)

        item_loader.add_value("landlord_name", "Donna Lloyd")
        item_loader.add_xpath("landlord_phone", "normalize-space(//span[@class='wpsight-listing-agent-phone'][1]/a/text())")
        item_loader.add_value("landlord_email", "blank@smithandelliott.com.au")

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