# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math

class MySpider(Spider):
    name = 'lavilleimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.laville-immo.fr/recherche/"
            }
            
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse)


    # 1. FOLLOWING
    def parse(self, response):

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//div[contains(@class,'selectionBien')]/article"):
            follow_url = response.urljoin(item.xpath(".//a[@class='titreArticleListing']/@href").get())
            if "location" in follow_url:
                prop_type = item.xpath(".//h3/text()").get()
                property_type = ""
                if "appartement" in prop_type.lower():
                    property_type = "apartment"
                elif "maison" in prop_type.lower():
                    property_type = "house"
                elif "studio" in prop_type.lower():
                    property_type = "apartment"
                elif "duplex" in prop_type.lower():
                    property_type = "apartment"
                elif "villa" in prop_type.lower():
                    property_type = "house"
                elif "immeuble" in prop_type.lower():
                    property_type = "house"
                if property_type != "":
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type' : property_type})
            seen = True
        
        if page == 2 or seen:
            url = f"https://www.laville-immo.fr/recherche/{page}"
            yield Request(
                url, 
                callback=self.parse, 
                meta={
                    'property_type' : response.meta.get('property_type'),
                    "page" : page+1
                }
            )
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath("title", "//h1[@class='titleBien']/text()")
        item_loader.add_value("external_source", "Lavilleimmo_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("external_link", response.url)

        price = response.xpath("//p[@class='price']/text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))
        
        external_id = response.xpath("//p[@class='ref']/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
       
        zipcode = response.xpath("//ul/li[@class='data'][contains(.,'Code postal')]/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(":")[1].strip() )
        
        address = response.xpath("//div[contains(@class,'content-info-quartier')]//h1//text()").extract_first()
        if address:
            item_loader.add_value("address", address.split("(")[0].strip() )
            item_loader.add_value("city", address.split("(")[0].strip() )

        room_count = response.xpath("//ul/li[@class='data'][contains(.,'pièce')]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split(":")[1].strip() )
       
        square = response.xpath("//ul/li[contains(.,'Surface')]/text()").extract_first()
        if square:
            square_meters=square.split(":")[1].split("m")[0].replace(",",".")
            square_meters = math.ceil(float(square_meters.replace(" ","").strip()))
            item_loader.add_value("square_meters",square_meters )
        
        floor = response.xpath("//ul/li[@class='data'][contains(.,'Etage')]/text()").extract_first()
        if floor:
            item_loader.add_value("floor",floor.split(":")[1].strip() )
           
        desc = "".join(response.xpath("//div[@class='offreContent']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "piscine" in desc:
                item_loader.add_value("swimming_pool", True)


        terrace = response.xpath("//ul/li[@class='data'][contains(.,'Terrasse')]/text()").extract_first()
        if terrace:
            if "NON" in terrace.upper():
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)
        
        parking = response.xpath("//ul/li[@class='data'][contains(.,'parking ')]/text()").extract_first()
        if parking:
            if "NON" in parking.upper():
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
       
        elevator = response.xpath("//ul/li[@class='data'][contains(.,'Ascenseur ')]/text()").extract_first()
        if elevator:
            if "NON" in elevator.upper():
                item_loader.add_value("elevator", False)
            else:
                item_loader.add_value("elevator", True)
        balcony = response.xpath("//ul/li[@class='data'][contains(.,'Balcon')]/text()").extract_first()
        if balcony:
            if "NON" in balcony.upper():
                item_loader.add_value("balcony", False)
            else:
                item_loader.add_value("balcony", True)

        map_coordinate = response.xpath("//script[contains(.,'lng')]/text()").extract_first()
        if map_coordinate:
            lat = map_coordinate.split('lat :')[1].split(',')[0].strip()
            lng = map_coordinate.split('lng:')[1].split('}')[0].strip()
            if lat:
                item_loader.add_value("longitude", lng)
            if lng:
                item_loader.add_value("latitude", lat)
       
        images = [response.urljoin(x) for x in response.xpath("//img[@class='img_Slider_Mdl']/@src").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "04 68 34 29 86")
        item_loader.add_value("landlord_name", "LAVILLE IMMO")
        yield item_loader.load_item()
