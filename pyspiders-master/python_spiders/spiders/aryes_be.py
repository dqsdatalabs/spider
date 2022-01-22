# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from scrapy.utils.url import add_http_if_no_scheme
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'aryes_be' 
    execution_type = 'testing' 
    country = 'belgium'
    locale = 'fr'
    external_source="Aryes_PySpider_belgium"
    start_urls = ["https://vente-location.aryes.be/"] #LEVEL-1
    custom_settings = {
        "HTTPCACHE_ENABLED": False,
    } 

    # 1. FOLLOWING
    def start_requests(self):
        url =  "https://vente-location.aryes.be/"
        yield Request(url,callback=self.jump)
    def jump(self,response):
        url=response.xpath("//a[contains(@href,'a-louer')]/@href").get()
        yield Request(url,callback=self.parse)
    def parse(self,response):
        a=response.xpath("//input[@name='_token']/@value").get()
        start_urls = [
            {
                "type": 0
            },
            {
                "type": 6
            },
            {
                "type": 12
            },
            {
                "type": 18
            },
            {
                "type": 24
            },
	     
        ] 
        for url in start_urls:
            r_type = str(url.get("type"))
            formdata = { 
            "_token":str(a),
            "purpose[]": "2",
            "type": "",
            "location": "",
            "sold": "0",
            "terrace": "",
            "garage": "",
            "garden": "",
            "parking": "",
            "start_index": r_type,
            "count": "6",
            "sort[column]": "updateDateTime",
            "sort[direction]": "false"
        }
            yield FormRequest(
                "https://vente-location.aryes.be/search",formdata=formdata,callback= self.parse_list,dont_filter=True)
    def parse_list(self, response):
        data=json.loads(response.body)
        for item in data['result']['result']:
            id=item['id']
            city=item['city']
            zip=item['zip']
            type=str(item['category']).split("'name':")[-1].replace('"',"").replace("'","").replace("}","").replace("%20","").strip()
            url=f"https://vente-location.aryes.be/a-louer/{type}/{zip}-{city}/{id}"
            yield Request(url,callback=self.populate_item,meta = {'dont_redirect': True,'handle_httpstatus_list': [302],'item':item})
    def populate_item(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link",response.url)
        item_loader.add_value("external_source",self.external_source)
        item=response.meta.get('item')

        dontallow=response.url
        if dontallow and "commerce" in dontallow:
            return 
        title=response.xpath("//div[@class='title-container']/h4/text()").get()
        if title:
            item_loader.add_value("title",title)
        dontallow=title 
        if dontallow and "bureaux" in dontallow:
            return 
        adres=item['address']
        if adres:
            item_loader.add_value("address",adres)
        square_meters=item['area']
        if square_meters:
            item_loader.add_value("square_meters",square_meters)
        try:
            bathroom_count=item['bathRooms']
            if bathroom_count:
                item_loader.add_value("bathroom_count",bathroom_count)
        except:
            pass
        try:
            type=str(item['category']).split("'name':")[-1].replace('"',"").replace("'","").replace("}","").replace("%20","").strip()
            if type and "maison" in type:
                item_loader.add_value("property_type","house")
            if type and "appartement" in type:
                item_loader.add_value("property_type","apartment")
        except:
            pass
        try:
            city=item['city']
            if city:
                item_loader.add_value("city",city)
        except:
            pass
        try:
            price=item['price']
            if price:
                item_loader.add_value("rent",price)
        except:
            pass 
        item_loader.add_value("currency","EUR")
        try:
            room=item['rooms']
            if room:
                item_loader.add_value("room_count",room)
        except:
            pass
        zipcode=item['zip']
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        try:

            energy_label=item['energyClass']
            if energy_label:
                item_loader.add_value("energy_label",energy_label)
        except:
            pass
       
        images=[x for x in response.xpath("//img[@class='img-responsive img-fullwidth']//@src").getall()]
        if images:
            item_loader.add_value("images",images)
      
        description=item['comments']
        if description:
            item_loader.add_value("description",description)
        try:
            parking=item['parking']
            if parking==1:
                item_loader.add_value("parking",True)
        except:
            pass
        item_loader.add_value("landlord_name","Aryes IMMOBILIER")
        yield item_loader.load_item()