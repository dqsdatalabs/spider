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
import dateparser

class MySpider(Spider):
    name = 'dailyimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):
        start_urls = [
            {"url": "https://www.dailyimmobilier.fr/immobilier/location-type/maison-categorie/1p-chambres/", "property_type": "house"},
            {"url": "https://www.dailyimmobilier.fr/immobilier/location-type/appartement-categorie/1p-pieces/", "property_type": "apartment"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@class='liste-items']/li/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        title = "".join(response.xpath("//div[@class='detail-offre-descriptif']/h3/text()").extract())
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("title", title.strip())
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Dailyimmobilier_PySpider_"+ self.country + "_" + self.locale)

        address_detail = response.xpath("//h2[@class='detail-header-titre']/text()[normalize-space()]").extract_first()
        if address_detail:
            address=address_detail.split("(")[0].strip()
            zipcode=address_detail.split("(")[1].split(")")[0].strip()
            ref=address_detail.split("réf.")[1].strip()
            if address:
                item_loader.add_value("address",address)
                item_loader.add_value("city",address)
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
            if ref:
                item_loader.add_value("external_id",ref)

        price = response.xpath("//p[@class='detail-offre-prix']//text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ","."))
       
        square = response.xpath("//h3/small[contains(.,'m²')]/text()").extract_first()
        if square:
            square_meters = square.split("-")[-1].split("m²")[0].strip().split(" ")[-1]
            item_loader.add_value("square_meters",square_meters )
            
        room_count = response.xpath("//ul[@class='detail-offre-liste-caracteristiques']/li[contains(.,'chambres')]//text()").extract_first()
        room = response.xpath("//h3/small[contains(.,'pièces')]/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count", room_count.split("chambre")[0].strip() )
        elif room:
            room = room.split("pièces")[0].strip().split(" ")[-1]
            item_loader.add_value("room_count", room)
        
        desc = "".join(response.xpath("//p[@class='detail-offre-texte']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "Ascenseur" in desc:
                item_loader.add_value("elevator", True)
        
        deposit = "".join(response.xpath("//ul/li[contains(.,'de garantie')]/text()").getall())
        if "de garantie :" in desc:
            deposit2 =  desc.split("de garantie :")[1].split("\u20ac")[0].replace(" ","")
            item_loader.add_value("deposit", deposit2)
        elif deposit:
            deposit = deposit.split("de garantie")[0].split("\u20ac")[0].strip().split(" ")[-1]
            item_loader.add_value("deposit", deposit)
            
        if "disponible" in desc:
            available_date = desc.split("disponible")[1].split("du")[1].split(".")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        
        energy = response.xpath("//div[contains(@class,'dpe-location')]/@class").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy.split("ges-location-")[1].upper())
        
        parking = response.xpath("//div[@class='detail-offre-descriptif']//li[contains(.,'parkings')]//text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//div[@class='detail-offre-descriptif']//li[contains(.,'Piscine')]//text()").extract_first()
        if furnished:
            item_loader.add_value("swimming_pool", True)
     
        terrace = response.xpath("//div[@class='detail-offre-descriptif']//li[contains(.,'terrasses')]//text()").extract_first()
        if terrace:
            item_loader.add_value("terrace", True) 

        lat = response.xpath("//div[@id='collapse1']/@data-latgps").extract_first()
        if lat:
            item_loader.add_value("latitude", lat)

        lng = response.xpath("//div[@id='collapse1']/@data-longgps").extract_first()
        if lng:
            item_loader.add_value("longitude", lng)

        images = [response.urljoin(x) for x in response.xpath("//div[@id='gallery-container']//a/@href").extract()]
        if images :
            item_loader.add_value("images", images)      

        item_loader.add_xpath("landlord_phone", "//div[@class='contact-box']/a[1]/text()")
        item_loader.add_xpath("landlord_name", "//div[@class='widget-testimonial-content']/h4//text()")
        item_loader.add_value("landlord_email", "contact@dailyimmobilier.fr")
        yield item_loader.load_item()