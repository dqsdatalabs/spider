# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'eefjevoogd_nl'
    start_urls = ['https://www.eefjevoogd.nl/woningen/huur/pm/?adress=&huurprijs%5Bmin%5D=&huurprijs%5Bmax%5D=&woonoppervlakte=&slaapkamers=&objecttype=appartement&orderby=status%3Aasc%2Ckoopprijs%3Adesc%2Chuurprijs%3Adesc']  # LEVEL 1
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl'
    external_source = 'Eefjevoogdmakelaardij_PySpider_netherlands_nl'
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='item_link']"):
            follow_url = item.xpath("./@href").extract_first()            
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//div[@class='nav-next pagination']/a/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        sale = response.xpath("//span[contains(., 'sale')]/text()").get()
        if sale:
            return
        
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//h1/text()")
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", "apartment") 
        external_id = response.xpath("//link[@rel='shortlink']//@href").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('?p=')[1])
        
        desc = "".join(response.xpath("//div[contains(@class, 'beschrijving')]//p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

            if 'parking' in desc.lower():
                item_loader.add_value('parking', True)
            if "service" in desc.lower():
                utilities = re.search(r"service [cost|charge].*(\d{2},\d{2})", desc.lower())
                if utilities:
                    item_loader.add_value("utilities", utilities.group(1).split(",")[0])

            if 'furnish' in desc.lower():
                item_loader.add_value("furnished", True)
            if 'lift' in desc.lower() or 'elevator' in desc.lower():
                item_loader.add_value("elevator", True)
        
        bathroom_count = response.xpath("//dt[contains(.,'bath')]/following-sibling::dd/text()").get() 
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)
        room_count = response.xpath("//dt[contains(.,'bedroom')]/following-sibling::dd/text()").get() 
        if room_count:
            item_loader.add_value("room_count", room_count)
        price = response.xpath("//span[@class='price meta_extra']/text()").re_first(r"\d.*\d")
        if price:
            item_loader.add_value("rent", price.replace(".",""))
        item_loader.add_value("currency", "EUR")
        
               
        square = response.xpath("//div[@class='col col-6 values']//text()[contains(.,'m')]").re_first(r'\d+')
        if square:
            item_loader.add_value("square_meters", square)
    
        addres = response.xpath("//dt[contains(.,'Adress')]/following-sibling::dd/text()").get()
        if addres:
            item_loader.add_value("address", addres)
        city = response.xpath("//dt[contains(.,'City')]/following-sibling::dd/text()").get()
        if city:
            item_loader.add_value("city", city)
        zipcode = response.xpath("//dt[contains(.,'Zip')]/following-sibling::dd/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode)
            
       
        floor = response.xpath("//dt[contains(.,'floor')]/following-sibling::dd/text()").get()
        if floor:
            item_loader.add_value("floor", floor)

        images = [x for x in response.xpath("//div[@class='images']/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)
        
        latlng = response.xpath("//script[@id='wonen-single-js-extra']//text()[contains(.,'latitude')]").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split('"latitude":')[1].split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split('"longitude":')[1].split(",")[0].strip())
        item_loader.add_value("landlord_phone", "020 3050560")
        item_loader.add_value("landlord_name", "Eefje Voogd Makelaardij")
        item_loader.add_value("landlord_email", "info@eefjevoogd.nl")        

        yield item_loader.load_item()
def split_address(address, get):
    zip_code = address.split(" ")[0]+" "+address.split(" ")[1]
    city = address.split(zip_code)[1].strip()

    if get == "zip":
        return zip_code
    else:
        return city