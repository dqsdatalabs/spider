# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
class MySpider(Spider):
    name = 'ms_immo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.ms-immo.com/locations?type_bien=1&ville=All&pieces=All&sort_by=created",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.ms-immo.com/locations?type_bien=2&ville=All&pieces=All&sort_by=created",
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
        for item in response.xpath("//a[@class='thumblink']/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})
        
        next_page = response.xpath("//a[.='›']/@href").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta['property_type']}
            )

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Ms_Immo_PySpider_france")
        external_id ="".join( response.xpath("//div[contains(@class,'group-desc-main')]/div/div[contains(.,'Référence ')]//text()").extract() )
        if external_id:
            item_loader.add_value("external_id",external_id.split(":")[1].strip())
        title ="".join( response.xpath("//h1[@class='page-header']//text()").extract() )
        if title:
            item_loader.add_value("title",title )
            item_loader.add_value("city",title.split("- ")[-1])
            if "Studio" in title:
                item_loader.add_value("property_type", "Studio")
            else:
                item_loader.add_value("property_type", response.meta.get('property_type'))
            if "garage" in title or "parking" in title:
                item_loader.add_value("parking", True)

        rent =" ".join(response.xpath("//div[contains(@class,'group-desc-main')]//div[@class='group-price']//text()[contains(.,'€')]").extract())
        if rent:     
           item_loader.add_value("rent_string", rent.replace(" ",""))   
        
        address =" ".join( response.xpath("//div[contains(@class,'group-desc-full')]/div/div[contains(.,'Adresse ')]//following-sibling::div//text()[normalize-space()]").extract() )
        if not address:
            address =" ".join( response.xpath("//div[contains(@class,'group-desc-full')]/div/div[contains(.,'Résidence')]//following-sibling::div//text()[normalize-space()]").extract() )
        if address:
            item_loader.add_value("address",address.strip())
       
        room_count = response.xpath("//div[contains(@class,'group-desc-full')]/div/div[contains(.,'Nombre de pièces')]//following-sibling::div//text()[normalize-space()]").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.strip())
         
        square ="".join( response.xpath("//div[contains(@class,'group-desc-full')]/div/div[contains(.,'Surface ')]//following-sibling::div//text()[normalize-space()]").extract() )  
        if square:
            square_meters =  square.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", square_meters) 
        available ="".join( response.xpath("//div[contains(@class,'group-desc-full')]/div/div[contains(.,'Date de disponibilité')]//following-sibling::div//text()[normalize-space()][not(contains(.,'Immédiate'))]").extract() )  
        if available:            
            available = dateparser.parse(available, languages=['fr'])
            if available:
                item_loader.add_value("available_date", available.strftime("%Y-%m-%d")) 

        utilities =" ".join(response.xpath("//div[contains(@class,'group-desc-main')]/div/div[contains(.,'charges mensuel')]//following-sibling::div//text()").extract())       
        if utilities:
            item_loader.add_value("utilities",utilities.replace(" ","")) 
        deposit =" ".join(response.xpath("//div[contains(@class,'group-desc-main')]/div/div[contains(.,'de garantie')]//following-sibling::div//text()").extract())       
        if deposit:
            item_loader.add_value("deposit",deposit.replace(" ","")) 
      
        desc = " ".join(response.xpath("//div[contains(@class,'field-name-field-desc-bien')]//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        energy = response.xpath("//img[contains(@class,'img-dpe img-dpe-energie')]//@alt").extract_first()       
        if energy:
            item_loader.add_value("energy_label", energy.split(":")[1]) 

        script_map =response.xpath("//script/text()[contains(.,'lat') and contains(.,'lon')]").extract_first()
        if script_map:  
            item_loader.add_value("latitude", script_map.split('"lat":')[1].split(",")[0])
            item_loader.add_value("longitude", script_map.split('"lon":')[1].split(",")[0])

        images = [response.urljoin(x) for x in response.xpath("//div[contains(@id,'-biens-field-photos-1-slider')]//div[@class='slide__content']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images)    

        item_loader.add_value("landlord_phone", " 04 92 52 31 88")
        item_loader.add_value("landlord_email", "msimmobilier@ms-immo.com")
        item_loader.add_value("landlord_name", "MS IMMOBILIER")    
 
        yield item_loader.load_item()