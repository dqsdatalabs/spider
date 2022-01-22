# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from typing import NewType
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
    name = 'karratharealestate_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Karratharealestate_PySpider_australia'
    custom_settings = {
    "HTTPCACHE_ENABLED": False
    }
    headers={
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "cookie": "_WHEELS_AUTHENTICITY=ULaO7W4MM5kryr%2BzTPlA12lhq45Zp%2FKhLHuIQumJfGX%2BnaxJMEAvOTKR0UwrjliNUeWPQmsiR0Nqh8c68o4JwbxpcVIP4JtbJ3YDPW9ssMRVAj1GPKEb2MeWdUXslRALluh4XmPzOTxTPsAv6cYLcA%3D%3D; FLASH=%7B%7D; _ga=GA1.3.262803822.1641879411; _gid=GA1.3.2142005567.1641879411; _fbp=fb.2.1641879412927.810179142; _gat=1",
        "referer": "https://www.karratharealestate.com.au/lease",
        "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Mobile Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
    }
    def start_requests(self):
        start_urls = [
            {"url": "https://www.karratharealestate.com.au/json/data/listings/?authenticityToken=xgRJZrqGTrl133ZTi%2B0sag%3D%3D&_method=post&input_types=&office_id=&listing_category=&staff_id=&postcode=&rental_features=&listing_sale_method=Lease&rental_features=&status=&listing_suburb_search_string=&listing_suburb_id=&surrounding_radius=6&listing_property_type=Apartment&LISTING_BEDROOMS=&LISTING_BATHROOMS=&CARPORTS=&LISTING_PRICE_FROM=&LISTING_PRICE_TO=&sort=date-desc&gallery&limit=12"},
            {"url": "https://www.karratharealestate.com.au/json/data/listings/?authenticityToken=xgRJZrqGTrl133ZTi%2B0sag%3D%3D&_method=post&input_types=&office_id=&listing_category=&staff_id=&postcode=&rental_features=&listing_sale_method=Lease&rental_features=&status=&listing_suburb_search_string=&listing_suburb_id=&surrounding_radius=6&listing_property_type=House&LISTING_BEDROOMS=&LISTING_BATHROOMS=&CARPORTS=&LISTING_PRICE_FROM=&LISTING_PRICE_TO=&sort=date-desc&gallery&limit=12"},
            {"url":"https://www.karratharealestate.com.au/json/data/listings/?authenticityToken=xgRJZrqGTrl133ZTi%2B0sag%3D%3D&_method=post&input_types=&office_id=&listing_category=&staff_id=&postcode=&rental_features=&listing_sale_method=Lease&rental_features=&status=&listing_suburb_search_string=&listing_suburb_id=&surrounding_radius=6&listing_property_type=House&LISTING_BEDROOMS=&LISTING_BATHROOMS=&CARPORTS=&LISTING_PRICE_FROM=&LISTING_PRICE_TO=&sort=date-desc&gallery&limit=12&pg=2"}
        ]  # LEVEL 1       
        for url in start_urls:
            yield Request(url=url.get('url'),callback=self.parse,headers=self.headers)
    # 1. FOLLOWING
    def parse(self, response):

        data1=str(response.body).split("listing_url")
        for i in range(0,len(data1)-1):
            itemm=json.loads(response.body)['data']['listings'][i]
            item=json.loads(response.body)['data']['listings'][i]['listing_url']
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={'item':itemm})

            
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item=response.meta.get('item')
        item_loader.add_value("property_type",item['listing_property_type'])
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("latitude",str(item['latitude']))
        item_loader.add_value('longitude',str(item['longitude']))
        adres=item['listing_street_address']
        if adres:
            item_loader.add_value("address",adres)
        id=item['id']
        if id:
            item_loader.add_value("external_id",str(id))
        rent=item['rentals_rent_pw']
        if id:
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","USD")
        images=item['listing_gallery']
        if images:
            item_loader.add_value("images",images)
        furnished=item['listing_furnished']
        if furnished and furnished=="1":
            item_loader.add_value("furnished",True)
        if furnished and furnished=="0":
            item_loader.add_value("furnished",False)
        room_count=item['listing_bedrooms']
        if room_count:
            item_loader.add_value("room_count",room_count)
        bathroom_count=item['listing_bathrooms']
        if bathroom_count:
            item_loader.add_value("bathroom_count",bathroom_count)
        pets_allowed=item['listing_pets']
        if pets_allowed and pets_allowed=="No":
            item_loader.add_value("pets_allowed",False)
        if pets_allowed and pets_allowed=="Yes":
            item_loader.add_value("pets_allowed",True)
        parking=item['listing_garages']
        if parking and parking=="1":
            item_loader.add_value("parking",True)
        description="".join(response.xpath("//div[@class='description cp__item']/p/text()").getall())
        if description:
            item_loader.add_value("description",description)
        deposit=response.xpath("//p[contains(.,'Bond')]/text()").get()
        if deposit:
            item_loader.add_value("deposit",deposit.split("-")[-1].replace("$","").replace(",",""))
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)

        yield item_loader.load_item()