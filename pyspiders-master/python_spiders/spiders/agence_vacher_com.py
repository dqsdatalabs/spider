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
    name = 'agence_vacher_com'    
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self, **kwargs):

        if not kwargs:
            kwargs = {"apartment":"appartement", "house":"maison"}

        for key, value in kwargs.items():
            formdata = {
                "trie": "ID-desc",
                "transaction": "louer",
                "modifabonnement": "0",
                "type_de_bien[]": value,
                "surface-de": "",
                "surface-a": "",
                "budget": "0-9999999",
            }
            yield FormRequest("https://www.vacher-habitat.com/louer/",
                            callback=self.parse,
                            formdata=formdata,
                            meta={'property_type': key})


    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='propertie_title']/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta['property_type']})

    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        
        item_loader.add_value("external_source", "Agence_Vacher_PySpider_france")
        external_id =response.xpath("substring-after(//div[@class='ref_propertie']/p//text(),':')").extract_first()
        if external_id:
            item_loader.add_value("external_id",external_id.strip())
        title ="".join(response.xpath("//div[@id='the-title']/h1//text()").extract())
        if title:
            item_loader.add_value("title",title )
            item_loader.add_value("zipcode",title.split(" - ")[-1].strip() )
            item_loader.add_value("city",title.split(" - ")[-2].strip() )
            item_loader.add_value("address",title.split(" - ")[-2].strip())
                        
        rent =" ".join(response.xpath("//div[@class='top_price_properties_block']//text()").extract())
        if rent:     
           item_loader.add_value("rent_string", rent.replace(" ",""))   
            
        room_count = response.xpath("//p[span[contains(.,'Nombre de pièce')]]/text()").extract_first() 
        if room_count:   
            item_loader.add_value("room_count",room_count.strip())
        bathroom_count = response.xpath("//p[span[contains(.,'Salle d')]]/text()").extract_first() 
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count.strip())
        square ="".join( response.xpath("//p[span[contains(.,'Surface')]]/text()").extract() )  
        if square:
            square_meters =  square.split("m")[0].strip().replace(",",".")
            item_loader.add_value("square_meters", int(float(square_meters))) 
        parking = response.xpath("//p[span[contains(.,'Parkings')]]/text()").extract_first() 
        if parking:   
            item_loader.add_value("parking",True)
        balcony = response.xpath("//p[span[contains(.,'Balcons')]]/text()").extract_first() 
        if balcony:   
            item_loader.add_value("balcony",True)
   
        utilities =" ".join(response.xpath("//div[@class='desc_propertie']/p//text()[contains(.,'Charges =')]").extract())       
        if utilities:
            utilities = utilities.split("Charges =")[1].split("€")[0].replace(" ","").replace(",",".")
            item_loader.add_value("utilities",int(float(utilities.strip()))) 

        deposit =" ".join(response.xpath("//div[@class='desc_propertie']/p//text()[contains(.,'Dépot de garantie')]").extract())       
        if deposit:
            deposit = deposit.split("Dépot de garantie")[1].split("€")[0].replace(" ","").replace(",",".").replace(":","").replace("=","")
            item_loader.add_value("deposit",int(float(deposit.strip()))) 
  
        desc = " ".join(response.xpath("//div[@class='desc_propertie' and h3[contains(.,'Descriptif')]]//p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        energy = response.xpath("//div[@class='dpe']/img[not(contains(@src,'dpe_default'))]/@src").extract_first()       
        if energy:
            item_loader.add_value("energy_label", energy.split("dpe_")[-1].split(".")[0].strip())
       
        item_loader.add_value("landlord_phone", "05 56 81 66 30")
        item_loader.add_value("landlord_name", "Agence Vacher Bordeaux") 

        images = [response.urljoin(x)for x in response.xpath("//div[@id='page-content']//div[@id='slidernivo2']//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
        
        yield item_loader.load_item()

