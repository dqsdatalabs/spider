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
    name = 'doncase_es'
    execution_type='testing'
    country='spain'
    locale='es'
    thousand_separator = ','
    scale_separator = '.'
     # LEVEL 1
    def start_requests(self):
        start_urls = [
            {"url": "http://www.doncasa.es/buscador/en_alquiler/pisos_duplex_apartamentos_aticos/?HabitacionesMinimas=&SuperficieMinima=&Referencia=&CampoOrden=precio&DireccionOrden=asc",
             "property_type": "apartment"
             }
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            })
    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 1)
        
        seen = False
        for item in response.xpath("//div[@class='media-body']//a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item,meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 1 or seen:
            url = f"http://www.doncasa.es/buscador/en_alquiler/pisos_duplex_apartamentos_aticos/?HabitacionesMinimas=&SuperficieMinima=&Referencia=&CampoOrden=precio&DireccionOrden=asc&Pagina={page}"
            yield Request(url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), "page": page+1})
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_xpath("title", "//h1[@class='titulo']/text()")
        

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Doncase_PySpider_"+ self.country + "_" + self.locale)
    
        external_id = response.xpath("//div[@class='cont-caract']//div[contains(.,'Referencia')]/span[2]/text()").extract_first()
        if external_id :
            item_loader.add_value("external_id", external_id)
       
  
        square_meters = response.xpath("//div[@class='cont-caract']//div[contains(.,'Superficie total')]/span[2]/text()").extract_first()
        if square_meters :            
            square_meters = math.ceil(float(square_meters.split("m")[0]))
            item_loader.add_value("square_meters", str(square_meters))
        
        room = response.xpath("//div[@class='cont-caract']//div[contains(.,'Habitaciones')]/span[2]/text()").extract_first()
        if room:
            item_loader.add_value("room_count",room.strip() )   
        
        floor = response.xpath("//div[@class='cont-caract']//div[contains(.,'Planta')]/span[2]/text()").extract_first()
        if floor :  
           item_loader.add_value("floor", floor.strip())

        rent = response.xpath("//div[@class='cont-caract']//div[contains(.,'Precio')]/span[2]/text()").extract_first()
        if rent :            
            item_loader.add_value("rent_string", rent.replace(".","").replace(" ",""))

        bathroom_count = response.xpath("normalize-space(//li/div[span[.='Baños']]/span[2]/text())").extract_first()
        if bathroom_count :            
            item_loader.add_value("bathroom_count",bathroom_count)
        
        elevator = response.xpath("//div[@class='cont-caract']//div[contains(.,'Ascensor')]/span[2]/text()").extract_first()
        if elevator :
            item_loader.add_value("elevator", True)
        balcony = response.xpath("//div[@class='cont-caract']//div[contains(.,'Balcón')]/span[2]/text()").extract_first()
        if balcony :
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[@class='cont-caract']//div[contains(.,'Terraza')]/span[2]/text()").extract_first()
        if terrace :
            item_loader.add_value("terrace", True)
       

        desc = "".join(response.xpath("//li[@class='descripcion']/span[@class='txt']/text()").extract())
        if desc :
            item_loader.add_value("description", desc.strip())
            if "amueblado" in desc.lower():
                item_loader.add_value("furnished", True)
            if 'piscina' in desc.lower():
                item_loader.add_value("swimming_pool", True)
            if 'admiten mascota' in desc.lower():
                item_loader.add_value("pets_allowed", True)
            if 'lavavajillas' in desc.lower():
                item_loader.add_value("dishwasher", True)
            if 'lavadora' in desc.lower():
                item_loader.add_value("washing_machine", True)

        item_loader.add_xpath("address", "//div[@class='cont-caract']//div[contains(.,'Dirección')]/span[2]/text()")    
        address = response.xpath("//div[@class='cont-caract']//div[contains(.,'Zona')]/span[2]/text()").extract_first()    
        if address :
            if "(" in address:
                city=address.split("(")[0]
                item_loader.add_value("city", city.strip())
                if ")" in address:
                    zipcode=address.split("(")[1].split(")")[0]

                    item_loader.add_value("zipcode", zipcode.strip())
            else:
                item_loader.add_value("city", address.strip())    
        lat_lng = "".join(response.xpath("//script[contains(.,'LatLng')]/text()").extract())
        if lat_lng:
            value=lat_lng.split("google.maps.LatLng(")[1].split(");")[0]
            lat=value.split(",")[0]
            lng=value.split(",")[1]
            if lat or lng:
                item_loader.add_value("latitude",lat)
                item_loader.add_value("longitude",lng)

        energy_label=response.xpath("//div[@class='emisiones']//div[contains(@class,'ce ')]/@class").extract_first()
        if energy_label :
            item_loader.add_value("energy_label", energy_label.split("ce ")[1])

        img=response.xpath("//div[@id='slider-detalle']//div[@class='item']//@data-src").extract() 
        if img:
            images=[]
            for x in img:
                image=x.split("('")[1].split("')")[0]
                images.append(image)
            if images:
                item_loader.add_value("images",  list(set(images)))
                item_loader.add_value("external_images_count",  len(images))
        
        item_loader.add_value("landlord_name", "DonCasa")
        item_loader.add_value("landlord_phone", "934589191") 
        item_loader.add_value("landlord_email", "bailen@doncasa.com") 

       
        
        yield item_loader.load_item()