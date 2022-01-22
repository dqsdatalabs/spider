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
import scrapy

class MySpider(Spider):
    name = 'howsy_com'
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    external_source = "Howsy_PySpider_united_kingdom"
    custom_settings = {"HTTPCACHE_ENABLED": False}
    start_urls = [
        "https://howsy.com/api/properties?location%5Blat%5D=53.4794892&location%5Blng%5D=-2.2451148&location%5Barea_limit%5D=20.703270260721&page=1&sort=-updated_at",
        "https://howsy.com/api/properties?location%5Blat%5D=52.4796992&location%5Blng%5D=-1.9026911&location%5Barea_limit%5D=15.207623954305046&page=1&sort=-updated_at",
        "https://howsy.com/api/properties?location%5Blat%5D=51.5073219&location%5Blng%5D=-0.1276474&location%5Barea_limit%5D=20.955856018469067&page=1&sort=-updated_at",
        "https://howsy.com/api/properties?location%5Blat%5D=53.4071991&location%5Blng%5D=-2.99168&location%5Barea_limit%5D=15.154245384399523&page=1&sort=-updated_at",
    ]
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, dont_filter=True)


    # 1. FOLLOWING
    def parse(self, response):
        data=json.loads(response.body)['data']
        for item in data:
            id=item['id']
            url=f"https://howsy.com/properties/{id}"
            yield Request(url, callback=self.populate_item,meta={"item":item})
  

    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source",self.external_source)
        item=response.meta.get('item')
        item_loader.add_value("external_link", response.url)

        external_id=item['id']
        if external_id:
            item_loader.add_value("external_id",str(external_id))
        address=item['public_address']
        if address:
            item_loader.add_value("address",address)
        city=item['city']
        if city:
            item_loader.add_value("city",city)
        zipcode=item['district_post_code']
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        latitude=item['latitude']
        if latitude:
            item_loader.add_value("latitude",latitude)
        longitude=item['longitude']
        if longitude:
            item_loader.add_value("longitude",longitude)
        room=item['bedroom_count']
        if room:
            item_loader.add_value("room_count",room)
        bath=item['bathroom_count']
        if bath:
            item_loader.add_value("bathroom_count",bath)
        description=item['description']
        if description:
            item_loader.add_value("description",description)
        rent=item['marketing_rent_per_month']
        if rent:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","GBP")
        feautures=item['favourite_features']
        for i in feautures:
            if "apartment" in str(i['display_name']).lower():item_loader.add_value("property_type","apartment")
            if "furnished"==str(i['display_name']):item_loader.add_value("furnished",True)
            if "Allocated parking"==str(i['display_name']):item_loader.add_value("parking",True)
        img=[]
        photos=item['relationships']['photos']
        if photos:
            for i in photos:
                imag=i['url']
                img.append(imag)
            item_loader.add_value("images",img)
        property_type=item_loader.get_output_value("property_type")
        if not property_type:
            property_type=item['relationships']['type']['display_name']
            if property_type and "Flat"==str(property_type):
                
                item_loader.add_value("property_type","apartment")
            if property_type and "Maisonette"==str(property_type):
                item_loader.add_value("property_type","house")


           


        yield item_loader.load_item()