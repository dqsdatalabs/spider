# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re
import math
from datetime import datetime

class MySpider(Spider):
    name = 'bimbenet_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source="Bimbenet_PySpider_france_fr"
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.bimbenet.fr/location/appartement?prod.prod_type=appt&page=1",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.bimbenet.fr/location/maison?prod.prod_type=house&page=1",
                ],
                "property_type": "house"
            }
            
        ]
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )
        
        
    # 1. FOLLOWING
    def parse(self, response):


        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='_gozzbg']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = response.url.replace(f"page={page-1}", f"page={page}")
            yield Request(
                url=url,
                callback=self.parse,
                meta={"page": page+1, 'property_type': response.meta.get('property_type')}
            )
         
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        
        item_loader.add_xpath("title", "//span[contains(@class,'_1kbczjr undefined')]/text()")
        title=response.xpath("//span[@class='_6ckos0 undefined textblock ']//text()").get()
        if title:
            item_loader.add_value("title",title)
        
        rent = response.xpath("//p[contains(.,'mois')]/text()").get()
        if rent :
            rent=rent.replace(" ","")
            item_loader.add_value("rent",  rent.strip())
        item_loader.add_value("currency", "EUR")
            
        if response.xpath("//span[contains(.,'Chambre')]/following-sibling::span/text()").get():
            item_loader.add_xpath("room_count", "//span[contains(.,'Pièces ')]//following-sibling::span//text()")
        else:
            room_count = response.xpath("//span[contains(.,'Pièce')]/following-sibling::span/text()").extract_first()
            if room_count :
                item_loader.add_value("room_count", room_count)
        
        square_meters = response.xpath("//span[contains(.,'Surface')]/following-sibling::span/text()").extract_first()
        if square_meters :           
            item_loader.add_value("square_meters", int(float(square_meters))) 
        
        city=response.xpath("//span[contains(.,'Localisation')]/following-sibling::span/text()").extract_first()
        if city:
            item_loader.add_value("city", city.strip())
            item_loader.add_value("address",city.strip())
        
        lat_lng = response.xpath("//script[contains(.,'latitude')]/text()").get()
        if lat_lng:
            latitude = lat_lng.split('latitude":"')[1].split('"')[0]
            longitude = lat_lng.split('longitude":')[1].split('"')[0]
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        bathroom = response.xpath("//span[contains(.,'Salle')]/following-sibling::span/text() |//span[contains(.,'Salle')]/following-sibling::span/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())

        info_text = response.xpath("//span[@class='_6ckos0 _5k1wy textblock ']/text()").get()
        if info_text:
            utilities = info_text.split(" €/mois")[0].split(" ")[-1].split(".")[0]
            item_loader.add_value("utilities",utilities)

            security_expanses = info_text.split(" €.")[0].split()[-1].split(".")[0]
            # TTC_expanses = info_text.split(" € TTC")[0].split()[-1].split(".")[0]
            deposit = int(security_expanses) #+ int(TTC_expanses)
            item_loader.add_value("deposit",deposit)
            
            

        
        external_id = response.xpath("//span[contains(.,'Référence')]/following-sibling::span/text()").extract_first()
        if external_id :
            item_loader.add_value("external_id", external_id)
                
        desc = "".join(response.xpath("//span[contains(@class,'_5k1wy textblock')]//text()").extract())
        if desc :
            item_loader.add_value("description", desc.strip())
        
        floor = response.xpath("//span[contains(.,'Étage')]/following-sibling::span/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        img=response.xpath("//script[contains(.,'image')]/text()").extract_first()
        if img :
            images= re.sub('\s{2,}', ' ', img.strip())
            img_json = json.loads(images)
            try:
                for i in img_json["offers"]["itemOffered"]["photo"]:
                    item_loader.add_value("images", i["url"])
            except: pass
        item_loader.add_value("landlord_name", "AGENCE BIMBENET")
        item_loader.add_value("landlord_phone", "02 38 52 28 38")
        
               
        yield item_loader.load_item()