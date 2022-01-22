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

class MySpider(Spider):
    name = 'dicampo_es'
    execution_type='testing'
    country='spain'
    locale='en'
    
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.dicampo.es/alquiler.php?limtipos=2799,1699,2999,3499,2899&buscador=1",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.dicampo.es/alquiler.php?limtipos=499,399,7599,999,199,6299&buscador=1",

                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.dicampo.es/alquiler.php?limtipos=3099&buscador=1",

                ],
                "property_type" : "studio"
            },
            

        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//article[@class='paginacion-ficha propiedad']/div[1]/a[@class='irAfichaPropiedad']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
            

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("external_source", "Dicampo_PySpider_"+ self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("title","//div[@class='fichapropiedad-tituloprincipal']/h1/text()")
        
        external_id=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Reference')]/span[2]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        room_count=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Bedrooms')]/span[2]/text()").get()
        room_c=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Type of property')]/span[2]/text()").get()
        if room_c=='Studio':
            item_loader.add_value("room_count", "1")
        elif room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count=response.xpath("//li[span[.='Bathrooms']]/span[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        address = response.xpath("//ul[@class='fichapropiedad-listadatos']/li[./span[contains(.,'Area / City')]]/span[@class='valor']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split("/")[0].strip())


        square_meters=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Net Internal Area')]/span[2]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split("m")[0].strip())
        
        rent=response.xpath("//div[@class='fichapropiedad-precio']/text()").get()
        if rent:
            rent = rent.split(' ')[0].replace(".","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
                  
        desc="".join(response.xpath("//section[@id='fichapropiedad-bloquedescripcion']/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        floor=response.xpath("//ul[@class='fichapropiedad-listadatos']/li[contains(.,'Level')]/span[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor)
        
        furnished=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Furniture')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            furnished=response.xpath("//li[span[.='Kitchen type']]/span[2]/text()[contains(.,'Furniture')]").get()
            if furnished:
                item_loader.add_value("furnished", True)
            
        elevator=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Elevator')]/text()").get()
        if elevator:
            item_loader.add_value("elevator", True)
            
        terrace=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Terrace')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
            
        parking=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking="".join(response.xpath("//li[@class='bloqueCalidadPropiedad']//text()[contains(.,'Garage')]").getall())
            if parking:
                item_loader.add_value("parking", True)

        
        swimming_pool=response.xpath("//ul[@class='fichapropiedad-listacalidades']/li/b[contains(.,'Pool')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
            
        images=[x for x in response.xpath("//div[@class='visorficha-miniaturas']/ul/li/@cargafoto").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        latitude_longitude=response.xpath("//script[contains(.,'listados.fichapropiedad')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('"latitud":')[1].split(",")[0].strip()
            longitude = latitude_longitude.split('"altitud":')[1].split(",")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        phone=response.xpath("//span[@class='datosagencia-telf']/text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        
        item_loader.add_value("landlord_name","Dicampo")
        item_loader.add_value("landlord_email","info@dicampo.es")

        status = response.xpath("//div[@class='visorficha-estadogestionadas']/span/text()").get()
        if not status:
            yield item_loader.load_item()