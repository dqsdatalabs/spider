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
import re

class MySpider(Spider):
    name = 'agencefavart_yonneimmo89_com' 
    execution_type='testing'
    country='france'
    locale='fr'  
    custom_settings = {
        "PROXY_ON": True,
    }

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://agencefavart.yonneimmo89.com/fr/locations", "property_type" : "apartment"
            }
            
        ] #LEVEL-1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse, meta={"property_type": url.get("property_type")})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[contains(@class,'fiches_liste-immo')]/article//a[@class='info_bulle']/@href").extract():
            follow_url = response.urljoin(item)
            
            yield Request(follow_url, callback=self.populate_item, meta={'property_type' : response.meta.get('property_type')})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = "".join(response.xpath("//div[@class='titre']/h2//text()").extract())
        item_loader.add_value("title", re.sub("\s{2,}", " ", title))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Agencefavart_yonneimmo89_PySpider_"+ self.country + "_" + self.locale)

        price = response.xpath("//div[@class='infos_generales']//p[contains(.,'mois')]/text()[normalize-space()]").extract_first()
        if price:
            item_loader.add_value("rent_string", price.replace(" ",""))
        
        external_id = response.xpath("//div[@class='bien_critere']/p/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())
     
        room_count = response.xpath("//div[@class='titre']/h2/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(" pi")[0].split(" ")[-1])

        bathroom_count = response.xpath("//li[contains(.,\"de salle d'eau\") or contains(.,'salle de bain')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(':')[-1].strip())
       
        square = response.xpath("//div[@class='bien_critere']//li[contains(.,'Surface habitable ')]/text()").extract_first()
        if square:
            square_meters=square.split(":")[1].split("m")[0].replace(",",".")
            square_meters = math.ceil(float(square_meters.strip()))
            item_loader.add_value("square_meters",square_meters )
        
        floor = response.xpath("//div[@class='infos_generales']//p[3]/text()[normalize-space()]").extract_first()
        if floor:
            item_loader.add_value("floor",floor.strip() )
        deposit = response.xpath("//div[@class='bien_critere']//li[contains(.,'Dépôt de garantie ')]/text()").extract_first()
        if deposit :
            item_loader.add_value("deposit",deposit.split(":")[1].split("€")[0].strip() )

        utilities = "".join(response.xpath("//div[@class='descriptif_detaille']/article/div/div/text()[contains(.,'€') or contains(.,'provision pour charges')]").extract())
        if utilities :
            item_loader.add_value("utilities",utilities.split("€")[0].strip() )
             
        desc = "".join(response.xpath("//div[@class='descriptif_detaille']//text()[normalize-space()]").extract())
        if desc:
            desc=re.sub("\s{2,}", " ", desc)
            item_loader.add_value("description", desc.replace("Descriptif détaillé","").strip())
        
        address = response.xpath("//div[@class='btn_detail']/h3/text()").extract()
        if address:
            zipcode=address[1].split("-")[0]
            if zipcode:
                item_loader.add_value("zipcode", zipcode.strip())
            city=address[1].split(" - ")[1]
            if city:
                item_loader.add_value("city", city.strip())
                address_name=address[0]+","+city
                item_loader.add_value("address", re.sub("\s{2,}", " ", address_name))
            else:
                item_loader.add_value("address", re.sub("\s{2,}", " ", address[0]))


        energy =response.xpath("//table[contains(@class,'tableau_conso')]/@class").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy.split("dpe_")[1].strip())
       
        parking = response.xpath("//div[@class='bien_critere']//li[contains(.,'parking')]/text()").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        
        furnished = response.xpath("//div[@class='bien_critere']//li[contains(.,'Meublé')]/text()").extract_first()
        if furnished:
            if "Non" in furnished:
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)

        map_coordinate = response.xpath("//script[contains(.,'var position')]/text()").extract_first()
        if map_coordinate:
            value=map_coordinate.split('position = [')[1].split(']')[0]
            lat = value.split(',')[0].strip()
            lng = value.split(',')[1].strip()
            if lat:
                item_loader.add_value("longitude", lng)
            if lng:
                item_loader.add_value("latitude", lat)
       
        images = [response.urljoin(x) for x in response.xpath("//span[@class='imgDetailA']/@data-fancybox-href").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        item_loader.add_value("landlord_phone", "03 86 62 15 72")
        item_loader.add_value("landlord_name", "Agence Favart")
        
        yield item_loader.load_item()
