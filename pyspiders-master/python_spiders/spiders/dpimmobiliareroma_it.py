# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import re
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'dpimmobiliareroma_it'
    execution_type='testing'
    country='italy'
    locale='it' 
    external_source = "Dpimmobiliareroma_PySpider_italy"

  
    def start_requests(self):
        url = "https://dpimmobiliareroma.it/affitto-casa-roma.asp" # LEVEL 1
        yield Request(url=url, callback=self.parse)

    def parse(self, response):
        for item in response.xpath("//div/a[@class='TitoloPagina']/@href").getall():
            url = response.urljoin(item)
            yield Request(url, callback=self.populate_item)
                
        pagination = response.xpath("//div/b/a[font]/following-sibling::a[1]/@href").get()
        if pagination:
            follow_url = response.urljoin(pagination)
            yield Request(follow_url, callback=self.parse)

        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        external_id=response.url
        if external_id:
            item_loader.add_value("external_id",external_id.split("idImmobile=")[-1].split("&")[0])
        
        prop = " ".join(response.xpath("//div/strong[.='Immobile:']/following-sibling::text()[1]").extract())
        if get_p_type_string(prop):
            item_loader.add_value("property_type", get_p_type_string(prop))
        else:  
            return
        item_loader.add_xpath("title", "//title/text()")

        rent=response.xpath("//strong[.='Prezzo:']/following-sibling::text()").get()
        if rent:
            rent=rent.strip() 
            item_loader.add_value("rent",rent)
        item_loader.add_value("currency","EUR")
        description="".join(response.xpath("//span[@itemprop='description']/ul/li/b/text() | //span[@itemprop='description']/b/text() | //span[@itemprop='description']/text() | //span[@itemprop='description']/font/b/text() | //span[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description",description)
        address=response.xpath("//strong[.='Indirizzo:']/following-sibling::a/u/text()").get()
        if address:
            item_loader.add_value("address",address)
        
        bathroom_count="".join(response.xpath("//span[@itemprop='description']/ul/li/b/text() | //span[@itemprop='description']/b/text() | //span[@itemprop='description']/text() | //span[@itemprop='description']/font/b/text() | //span[@itemprop='description']//text()").getall())
        if bathroom_count:
            if "bagni" in bathroom_count.lower():
                room=bathroom_count.split("bagni")[0].split("Bagni")[0].split(",")[-1]
                if room:
                    if "quattro" in room.lower():
                        item_loader.add_value("bathroom_count","4")
                    if "due" in room.lower():
                        item_loader.add_value("bathroom_count","2")
                    if "tre" in room.lower():
                        item_loader.add_value("bathroom_count","3")
                    else:
                        
                        item_loader.add_value("bathroom_count",room.strip().split(" ")[0])
                    
        square_meters=response.xpath("//strong[.='Mq:']/following-sibling::text()").get()
        if square_meters:
            item_loader.add_value("square_meters",square_meters.strip())
        room_count="".join(response.xpath("//span[@itemprop='description']/ul/li/b/text() | //span[@itemprop='description']/b/text() | //span[@itemprop='description']/text() | //span[@itemprop='description']/font/b/text() | //span[@itemprop='description']//text()").getall())
        if room_count:
            if "camere" in room_count.lower():
                room=room_count.split("camere")[0].split(",")[-1]
                if room:
                    if "quattro" in room:
                        item_loader.add_value("room_count","4")
                    if "due" in room:
                        item_loader.add_value("room_count","2")
                    if "tre" in room:
                        item_loader.add_value("room_count","3")
                    else:
                        
                        item_loader.add_value("room_count",room.strip().split(" ")[0])
                

            
        # latitude=response.xpath("//input[@name='latitud']/@value").get()
        # if latitude:
        #     item_loader.add_value("latitude",latitude)
        # longitude=response.xpath("//input[@name='longitud']/@value").get()
        # if longitude:
        #     item_loader.add_value("longitude",longitude)
        images=[response.urljoin(x) for x in response.xpath("//div[@id='gallery']/ul//li/a/@href").getall()]
        if images:
            item_loader.add_value("images",images)
        landlord_name="DP Immobiliare Roma"
        item_loader.add_value("landlord_name",landlord_name)
        landlord_phone="+39.06.89537399"
        item_loader.add_value("landlord_phone",landlord_phone)
        landlord_email="dpimmobiliareroma@gmail.com"
        item_loader.add_value("landlord_email",landlord_email)

        yield item_loader.load_item()

def get_p_type_string(p_type_string):
    if p_type_string and "camere" in p_type_string.lower():
        return "room"
    elif p_type_string and "studio" in p_type_string.lower():
        return "studio"
    elif p_type_string and ("appartamento" in p_type_string.lower()):
        return "apartment"
    elif p_type_string and ("house" in p_type_string.lower()):
        return "house"
    else:
        return None