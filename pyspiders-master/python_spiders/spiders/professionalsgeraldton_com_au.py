# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re 

class MySpider(Spider):
    name = 'professionalsgeraldton_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Professionalsgeraldton_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.professionalsgeraldton.com.au/search/?show_in_rental=true&sold=0&order_by=listing_date&order=desc"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,)
    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        border=10
        seen = False
        for item in response.xpath("//div[@class='row']//div[@class='col-md-4']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
            seen=True
        if page == 2 or seen:
            if page<border:
                nextpage=f"https://www.professionalsgeraldton.com.au/search/?show_in_rental=true&sold=0&order_by=listing_date&order=desc&pageno={page}"
                if nextpage:
                    yield Request(response.urljoin(nextpage), callback=self.parse,meta={'page':page+1})
            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        adres=response.xpath("//div[@class='address']/text()").get()
        if adres:
            item_loader.add_value("address",adres)
      
        rent=response.xpath("//div[@class='price']/text()").get()
        if rent and not "Offers" in rent:
            if not "Under" in rent: 
                rent=rent.split("$")[-1].split("per")[0].replace(" ","").replace("\xa0","").strip().split("+")[0].replace(",","")
                if rent:
                    item_loader.add_value("rent",rent)
        if "Warehouse" in rent or "Shopping" in rent or "restored" in rent or "Under" in rent or "Application" in rent or "Offers " in rent:
            return 
        dontallow=response.xpath("//div[@class='tabbed-content']//h1/text()").get()
        if dontallow and ("Restaurant" in dontallow or "Shop" in dontallow or "Storage" in dontallow):
            return 

        item_loader.add_value("currency","USD")
        description="".join(response.xpath("//div[@class='property-heading']/following-sibling::p/text()").getall())
        if description:
            item_loader.add_value("description",description.replace("\n","").replace(" ",""))
        external_id=response.xpath("//text()[contains(.,'Property ID:')]/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        deposit=response.xpath("//text()[contains(.,'Bond:')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split(":")[-1].replace("$","").replace(",",""))
        room_count=response.xpath("//img[@class='bed']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=response.xpath("//img[@class='bath']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        parking=response.xpath("//img[@class='car']/following-sibling::text()").get()
        if parking:
            item_loader.add_value("parking",True)
        property_type=response.xpath("//div[@class='property-attributes-wrapper']/div/text()").get()
        if property_type and ("House" in property_type or "Duplex" in property_type):
            item_loader.add_value("property_type","house")
        if property_type and "Unit" in property_type:
            item_loader.add_value("property_type","apartment")
        if "Shop" in property_type or "Offices" in property_type or "Industrial" in property_type:
            return 
        square_meters=response.xpath("//sup[.='2']/parent::div/i/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m")[0].strip())
        images=[x for x in response.xpath("//div[@data-fancybox='image-gallery']/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        item_loader.add_value("landlord_name","Professionals Real Estate")
        item_loader.add_value("landlord_phone","(08) 9965 2000")


        yield item_loader.load_item()