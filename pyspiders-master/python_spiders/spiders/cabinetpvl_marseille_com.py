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
    name = 'cabinetpvl_marseille_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.cabinetpvl-marseille.com/location-maison.html",
                "property_type" : "house"
            },
            {
                "url" : "https://www.cabinetpvl-marseille.com/location-appartement.html",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url= url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})
            
    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//article[@class='listing-thumbnail']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
           
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Cabinetpvlmarseille_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))  
        item_loader.add_value("external_link", response.url)
        title = "".join(response.xpath("//h2[contains(@class,'detail')]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title",title)
            address = title.split(" -")[1].strip()
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("(")[0].strip())
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])
            
            if "réf." in title:
                external_id = title.split("réf.")[1].strip().split(" ")[0]
                item_loader.add_value("external_id", external_id)
            
        rent=response.xpath("//p[contains(@class,'prix')]/text()").get()
        if rent:
            price=rent.replace(" ","")
            item_loader.add_value("rent_string", price)
        
        square_meters=response.xpath("//h3/small/text()[contains(.,'m²')]").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m²")[0].strip().split(" ")[-1])
        
        desc="".join(response.xpath("//p[contains(@class,'offre-texte')]//text()").getall())
        
        room_count=response.xpath("//h3/small/text()[contains(.,'pièce')]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("pièce")[0].strip().split(" ")[-1])
        elif "chambre" in desc:
            room_count = desc.split("chambre")[0].strip().split(" ")[-1]
            if room_count.isdigit():
                item_loader.add_value("room_count", room_count)
        
        latitude_longitude = response.xpath("//script[contains(.,'lng: Number(')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('lat: Number(')[-1].split(')')[0]
            longitude = latitude_longitude.split('lng: Number(')[-1].split(')')[0].strip()           
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        if desc:
            item_loader.add_value("description", desc.strip())
            
        images=[x for x in response.xpath("//div[contains(@id,'gallery')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
            
        utilties=response.xpath("//li//text()[contains(.,'de provisions')]").get()
        if utilties:
            utilties = utilties.split("de provisions")[0].split("€")[0].strip().split(" ")[-1]
            item_loader.add_value("utilities", utilties)
        
        deposit=response.xpath("//li//text()[contains(.,'dépôt de garantie')]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split("€")[0].strip())

        item_loader.add_value("landlord_name", "CABINET P.V.L")
        item_loader.add_value("landlord_phone", "+33 (0)4 91 09 13 93")
        item_loader.add_value("landlord_email", "cabinetpvl@gmail.com")
        
        yield item_loader.load_item()