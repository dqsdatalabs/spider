# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

# from tkinter.font import ROMAN
from parsel.utils import extract_regex
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
import re

class MySpider(Spider):
    name = 'concept-home_be'
    execution_type='testing'
    country='belgium'
    locale='nl'
    external_source='Concepthome_PySpider_belgium'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://concept-home.be/Modules/ZoekModule/RESTService/SearchService.svc/GetPropertiesCount/3/0/0/2/0/0/0/0/0/0/0/0/0/0/0/28/0/false/0/NL/0/0/0/1/DEHMJVBXLIWSUPXYWROSKUBBTVPGWFRYVZSNFTKWMFRVSRKBVC/0/0/0/1/0/0/false/false/0/false/0/0/0/false/0/0?_=1642149591647"},

        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                        
                        )
    # 1. FOLLOWING
    def parse(self, response):

        sel = Selector(text=response.body, type='xml')
        for itemmain in sel.xpath("//Property_Management_Overview"):
            item=itemmain.xpath(".//Property_URL/text()").get()
            follow_url = f"https://concept-home.be/nl{item}"
            yield Request(follow_url, callback=self.populate_item,meta={'item':itemmain})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
 
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item=response.meta.get('item')
        external_id=item.xpath(".//FortissimmoID/text()").get()
        if external_id:
            item_loader.add_value("external_id",external_id)
        for i in ["",1,2,3,4,5]:
            images=item.xpath(f".//Image_URL{i}/text()").get()
            if images:
                item_loader.add_value("images",images)
        descripiton=item.xpath(".//Property_Description/text()").get()
        if descripiton:
            item_loader.add_value("description",descripiton)
        property_type=item.xpath(".//Property_HeadType_Value/text()").get()
        if property_type and "Appartement"==property_type:
            item_loader.add_value("property_type","apartment")
        if property_type and "Huis"==property_type:
            item_loader.add_value("property_type","house")
        latitude=item.xpath(".//Property_Lat/text()").get()
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=item.xpath(".//Property_Lon/text()").get()
        if longitude:
            item_loader.add_value("longitude",longitude)
        rent=item.xpath(".//Property_Price/text()").get()
        if rent:
            item_loader.add_value("rent",rent.split("€")[-1].replace(" ","").replace("\xa0","").strip())
        item_loader.add_value("currency","EUR")
        title=item.xpath(".//Property_Title/text()").get()
        if title:
            item_loader.add_value("title",title)
        zipcode=item.xpath(".//Property_Zip/text()").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        room_count=item.xpath(".//bedrooms/text()").get()
        if room_count:
            item_loader.add_value("room_count",room_count)
        square_meters=item.xpath(".//Property_Area_Build/text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.split("m²")[0])
        city=item.xpath(".//Property_City_Value/text()").get()
        if city:
            item_loader.add_value("city",city)
        item_loader.add_value("landlord_name","Concept Home Immo")
        yield item_loader.load_item()