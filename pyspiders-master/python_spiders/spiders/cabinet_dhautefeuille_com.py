# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'cabinet_dhautefeuille_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://cabinet-dhautefeuille.com/advanced-search/?type=appartement&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://cabinet-dhautefeuille.com/advanced-search/?type=maison&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://cabinet-dhautefeuille.com/advanced-search/?type=studio&max-price=&status=location&bathrooms=&min-area=&max-area=&min-price=&max-price=&property_id=",
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
        for item in response.xpath("//a[@class='hover-effect']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[@rel='Next']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Cabinet_Dhautefeuille_PySpider_france")
        item_loader.add_xpath("title", "//div/h1//text()")
 
        address =response.xpath("//div[@class='detail-address-inner']//span[span[.='Lieu : ']]/span[2]//text()").extract_first()
        if address:
            item_loader.add_value("address",address.strip() ) 
            item_loader.add_value("city",address.strip() ) 
        zipcode =response.xpath("//li[@class='detail-zip']/text()").extract_first()
        if zipcode:
            item_loader.add_value("zipcode",zipcode.strip() ) 
                
        item_loader.add_xpath("external_id","//div[@class='detail-address-inner']//span[span[.='Référence : ']]/span[2]//text()")                
        item_loader.add_xpath("deposit","//li/div[strong[contains(.,'Dépôt de Garantie')]]/label/text()")                
                       
        utilities =response.xpath("//li/div[strong[contains(.,'sur Charges')]]/label/text()").extract_first()
        if utilities:
            item_loader.add_value("utilities",utilities.strip() )      
        else:
            utilities =response.xpath("substring-after(//div[@id='description']/p//text()[contains(.,' charge')],' charge')").extract_first()
            if utilities:
                item_loader.add_value("utilities",utilities.strip() ) 
              
        rent =response.xpath("//div[@class='header-detail']//span[@class='item-price']//text()").extract_first()
        if rent:     
           item_loader.add_value("rent_string", rent)   

        floor = response.xpath("//li/div[strong[contains(.,'Etage ')]]/label/text()").extract_first() 
        if floor:   
            item_loader.add_value("floor",floor.strip())     
        room_count = response.xpath("//div[@class='detail-address-inner']//span[span[.='Chambres : ']]/span[2]//text()").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.strip())
        elif "studio" in response.meta.get('property_type') :
            item_loader.add_value("room_count","1")
        else:
            room_count = response.xpath("substring-before(//li/div[strong[contains(.,'Pièces')]]/label/text(),'pièce')").extract_first() 
            if room_count:   
                item_loader.add_value("room_count",room_count.strip())
                
        bathroom_count = response.xpath("//li/div[strong[contains(.,'de Bain')]]/label/text()").extract_first() 
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count.strip())

        square =response.xpath("//div[@class='detail-address-inner']//span[span[.='Surface : ']]/span[2]//text()").extract_first()
        if square:
            square_meters =  square.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters) 

        energy = response.xpath("substring-after(//h5[contains(.,'DPE')]//text(),':')").extract_first()
        if energy:
            item_loader.add_value("energy_label", energy.split("(")[0].strip())
    
        desc = " ".join(response.xpath("//div[@id='description']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
      
        parking = response.xpath("//div[@id='features']//li//text()[contains(.,'Parking')]").extract_first()
        if parking:
            item_loader.add_value("parking", True)
        elevator = response.xpath("//div[@id='features']//li//text()[contains(.,'Ascenseur')]").extract_first()
        if elevator:
            item_loader.add_value("elevator", True)      
        images = [x for x in response.xpath("//div[@class='gallery-inner']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)

        item_loader.add_value("landlord_name", "CABINET D'HAUTEFEUILLE")
        item_loader.add_value("landlord_phone","0322717515")
        item_loader.add_value("landlord_email","location@dhautefeuille.fr")
  
        yield item_loader.load_item()