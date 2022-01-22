# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin 
import re

class MySpider(Spider):
    name = 'immoval_com'    
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Immoval_PySpider_france_fr"
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.immoval.com/wp-json/immoval/property/search?project=rent&type=house&sort=relevancy&distance=10&_=1603969827779",
                "property_type" : "house"
            },
            {
                "url" : "https://www.immoval.com/wp-json/immoval/property/search?project=rent&type=apartment&sort=relevancy&distance=10&_=1603969827782",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
    
        for item in json.loads(response.body):
            f_url = item["permalink"]
            lat = item["latitude"]
            lng = item["longitude"]
            #item["ref"]
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"item":item,
                    "property_type" : response.meta.get("property_type"),
                    "lat" : lat,
                    "lng" : lng,
                },
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response) 
        
        
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)

        item_loader.add_value("latitude", response.meta.get('lat'))
        item_loader.add_value("longitude", response.meta.get('lng'))
        item=response.meta.get('item')
        property_type = response.meta.get('property_type') 
        title = item["title"]
        if title:
            item_loader.add_value("title",title )
            if "studio" in title.lower():
                property_type = "studio"
        title1= item["title"]

        if "commercial" in title1.lower():
            return 

        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_id", item["ref"])
        item_loader.add_value("rent", item["price"])
        item_loader.add_value("zipcode", item["postalcode"])
        item_loader.add_value("city",item["city"])
        
        desc = "".join(response.xpath("//div[@class='property__description']//text()").extract())
        if desc:
            item_loader.add_value("description", re.sub("\s{2,}", " ", desc))
            try:
                if "Provision sur charges" in desc:
                    utilities = desc.split("Provision sur charges")[1].split("€")[0].strip()
                    if utilities !="0":
                        item_loader.add_value("utilities",utilities.replace(".",""))
                elif "mois de charges" in desc:
                    utilities = desc.split("mois de charges")[0].replace("€", "").replace("/", "").strip().split(" ")[-1]
                    item_loader.add_value("utilities", utilities)
                if " de garantie" in desc:
                    deposit = desc.split(" de garantie")[1].split("€")[0].strip()
                    if deposit !="0":
                        item_loader.add_value("deposit",deposit.replace(".","").replace(" ",""))
            except:
                pass

        # if item["rooms"]=="0":
        #     item_loader.add_value("room_count", desc.split("pces")[0].strip().split(" ")[-1])
        # else:
        #     item_loader.add_value("room_count", item["rooms"])
        room=response.xpath("//div[@class='table__cell']/strong[contains(.,'Nb de pièce(s)')]/following-sibling::span//text()").get()
        if room:
            item_loader.add_value("room_count",room.strip())
            
            
        item_loader.add_value("address", item["address"])
        item_loader.add_value("square_meters", item["total_area"])
        item_loader.add_value("landlord_name", item["agency_name"])
        item_loader.add_value("currency", "EUR")
        bathroom = response.xpath("//div[@class='table__cell']/strong[contains(.,'Nb de salle')]/following-sibling::span/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip().split(" ")[0])

        # images=[]
        # images.append(item["thumbnail"])
        # item_loader.add_value("images", images)

        images=[x for x in response.xpath("//div//ul[@class='cover__thumbs']/li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_xpath("floor", "//div[@class='table__cell']/strong[.='Étage']/following-sibling::span/text()")

        item_loader.add_value("landlord_phone", "0388228822")
        item_loader.add_value("landlord_email", "immoval@immoval.com")

        elevator = item["elevator"]
        if elevator:
            if elevator=="1":
                item_loader.add_value("elevator", True)
            else:
                item_loader.add_value("elevator", False)

        balcony = item["balcony"]
        if balcony:
            if balcony=="1":
                item_loader.add_value("balcony", True)
            else:
                item_loader.add_value("balcony", False)
        
        furnished = item["furnished"]
        if furnished:
            if furnished=="1":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)
        
        parking = item["parking"]
        garage = item["garage"]
        if parking or garage:
            if parking=="1" or garage=="1":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", False)
        
        energy = response.xpath("//div[@class='column--half' and contains(.,'énergétique')]/div/@data-index").get()
        if energy:
            item_loader.add_value("energy_label", energy.strip())
        yield item_loader.load_item()
