# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re

class MySpider(Spider):
    name = 'tourimmo_net'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Tourimmo_PySpider_france"

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.tourimmo.net/immobilier/location/appartement/partout/?lsi_s_extends=0",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.tourimmo.net/immobilier/location/maison/partout/?lsi_s_extends=0",
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='all-bien-container']/div"):
            follow_url = response.urljoin(item.xpath(".//a/@href").get())
            is_studio = item.xpath(".//h2/text()[contains(.,'Studio')]").get()
            if is_studio: yield Request(follow_url, callback=self.populate_item, meta={"property_type":"studio"})
            else: yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

        next_button = response.xpath("//a[contains(.,'Suivante') and not(contains(@href,'#'))]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Tourimmo_PySpider_france")
        external_id = response.xpath("//p[@class='mandate']/text()").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[-1].strip())
        title = " ".join(response.xpath("//div[@class='title-big']//text()").extract())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title)) 
        address = " ".join(response.xpath("//div[@id='adtextlsiwidget-3']//h2/span/span//text()").extract())
        if address:
            item_loader.add_value("address",address.strip()) 
        city = response.xpath("//h2//span[@class='city']/text()").extract_first()
        if city:
            item_loader.add_value("city",city.strip()) 
        zipcode = response.xpath("//h2//span[@class='zipCode']/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.replace("(","").replace(")","").strip()) 

        room_count = response.xpath("//li[strong[contains(.,'Nbre. de chambres')]]//span[@class='value']/text()").extract_first()
        if room_count:
            item_loader.add_value("room_count",room_count.strip()) 
        else:
            room_count = response.xpath("//li[strong[contains(.,'Nb. de pièces')]]//span[@class='value']/text()").extract_first()
            if room_count:
                item_loader.add_value("room_count",room_count.strip()) 
        
        item_loader.add_xpath("bathroom_count", "//li[strong[contains(.,'Nb. de salles d')]]//span[@class='value']/text()")
        item_loader.add_xpath("floor", "//li[strong[contains(.,'Étage du')]]//span[@class='value']/text()")

        energy_label = response.xpath("//div[contains(@class,'dpe-conso-en')]//div[@class='letter']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label",energy_label)

        rent = response.xpath("//div[@class='post_content']//span[contains(@class,'price')]").extract_first()
        if rent:    
            item_loader.add_value("rent_string",rent.replace(" ","").replace('\xa0', ''))  
           
        square = response.xpath("//li[strong[.='Surface habitable']]//span[@class='value']/text()").extract_first()
        if square:
            item_loader.add_value("square_meters",square.split("m")[0].strip()) 

        deposit = response.xpath("//li[strong[.='Dépôt de garantie']]//span[@class='value']/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit",deposit.replace(" ","").strip()) 

        utilities = response.xpath("//li[strong[.='Charges']]//span[@class='value']/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities",utilities.replace(" ","")) 

        parking = response.xpath("//li/strong[.='Parking' or .='Garage']//text()").extract_first()    
        if parking:
            item_loader.add_value("parking", True)
        terrace = response.xpath("//li/strong[.='Terrasse']//text()").extract_first()    
        if terrace:
            item_loader.add_value("terrace", True)
        balcony = response.xpath("//li/strong[.='Balcon']//text()").extract_first()    
        if balcony:
            item_loader.add_value("balcony", True)
        furnished = response.xpath("//li/strong[.='Meublé']//text()").extract_first()    
        if furnished:
            item_loader.add_value("furnished", True)
        elevator = response.xpath("//li/strong[.='Ascenseur']//text()").extract_first()    
        if elevator:
            item_loader.add_value("elevator", True)
     
        desc = " ".join(response.xpath("//p[@class='description']//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
     
        images = [response.urljoin(x) for x in response.xpath("//div[@class='flexslider carousel']/ul/li/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "TOUR IMMO")
        item_loader.add_value("landlord_phone", "04 78 48 88 28")
        item_loader.add_value("landlord_email", "contact@tourimmo.net")

        yield item_loader.load_item()