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
import math

class MySpider(Spider):
    name = 'presidence_es'
    execution_type='testing'
    country='spain'
    locale='es'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.presidence.es/results/?id_tipo_operacion=2&modo=&od=&dt%5B%5D=&precio_min=&precio_max=&m2_min=&m2_max=&dormitorios_min=&dormitorios_max=&banos_min=&banos_max=&type%5B%5D=1&type%5B%5D=18&type%5B%5D=21&type%5B%5D=22&tipos_obra=&t_piscina=&fecha_alta=&vistas=&tipo_calefaccion=&primera_linea=&amueblado=&de_banco=&piscina=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.presidence.es/results/?id_tipo_operacion=2&modo=&od=&dt%5B%5D=&precio_min=&precio_max=&m2_min=&m2_max=&dormitorios_min=&dormitorios_max=&banos_min=&banos_max=&type%5B%5D=3&tipos_obra=&t_piscina=&fecha_alta=&vistas=&tipo_calefaccion=&primera_linea=&amueblado=&de_banco=&piscina=",

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

        for item in response.xpath("//div[contains(@class,'swiper-slide')]/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='next']/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )
            
        

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rented = response.xpath("//div[@id='etiqueta']//span/text()[.='Alquilado']").extract_first()
        if rented:
            return

        property_type = response.meta.get('property_type')    
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Presidence_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("title", "//div[@class='headerLeft']/h1/text()")
        item_loader.add_xpath("external_id", "//div[@class='headerLeft']/p/span/text()")
        
        square_meters = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Útil')]/text()[normalize-space()]").extract_first()
        if square_meters :            
            square_meters = math.ceil(float(square_meters.split("m")[0]))
            item_loader.add_value("square_meters", str(square_meters))
        else:
            square_meters = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Construida')]/text()[normalize-space()]").extract_first()
            if square_meters :
                square_meters = math.ceil(float(square_meters.split("m")[0]))
                item_loader.add_value("square_meters", str(square_meters))
                
        room = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Habitaciones: ')]/text()[normalize-space()]").extract_first()
        if room:
            item_loader.add_value("room_count",room)   
        bath_room = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Baños')]/text()[normalize-space()]").extract_first()
        if bath_room:
            item_loader.add_value("bathroom_count",bath_room)  
        floor = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Planta')]/text()[normalize-space()]").extract_first()
        if floor :  
            item_loader.add_value("floor", floor.replace("º","").strip())
        else:
            floor2 = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Floor')]/text()[normalize-space()]").extract_first()
            if floor2 :  
                item_loader.add_value("floor", floor2.replace("º","").strip())


        rent = response.xpath("//p[@class='precio']//text()").extract_first()
        if rent :         
            item_loader.add_value("rent_string", rent.strip())
        
        elevator = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Ascensor')]/text()[normalize-space()]").extract_first()
        if elevator :
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Terraza')]/text()[normalize-space()]").extract_first()
        if terrace :
            item_loader.add_value("terrace", True)


        balcony = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Balcony: ')]/text()[normalize-space()]").extract_first()
        if balcony :
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Amueblado')]/text()[normalize-space()]").extract_first()  
        if furnished:
            item_loader.add_value("furnished", True)

        desc = "".join(response.xpath("//div[@id='descripcionFicha2']/p//text()").extract())
        if desc :
            item_loader.add_value("description", desc.strip())
     
        address = ",".join(response.xpath("//div[@class='detallesFicha']//li[contains(.,'Población') or contains(.,'Zona') ]/text()[normalize-space()]").extract() )   
        if address :
            item_loader.add_value("address",address)
        
        city = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Población')]/text()[normalize-space()]").extract_first()
        if city:
            item_loader.add_value("city", city.strip())    

        swimming_pool = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Piscina')]/text()[normalize-space()]").extract_first()
        if swimming_pool :
            item_loader.add_value("swimming_pool", True)

        parking = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Parking') or contains(.,'Garaje')]/text()[normalize-space()]").extract_first()
        if parking :
            item_loader.add_value("parking", True)

        pet = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Admite mascotas')]/text()[normalize-space()]").extract_first()
        if pet :
            item_loader.add_value("pets_allowed", True)
                    
        energy_label=response.xpath("//div[@class='detallesFicha']//li[contains(.,'energética (consumo)') or contains(.,'energética (emisiones)')]/text()[normalize-space()][.!=' NO ']").extract_first()
        if energy_label  :
            item_loader.add_value("energy_label",energy_label.strip() )
      
        img=response.xpath("//div[@class='fotorama']//a/@href").extract() 
        if img:
            images=[]
            for x in img:
                images.append(x)
            if images:
                item_loader.add_value("images",  list(set(images)))

        item_loader.add_xpath("latitude","//div[@id='mapa']/@data-lat")
        item_loader.add_xpath("longitude","//div[@id='mapa']/@data-lng")

        item_loader.add_value("landlord_name", "PRESIDENCE")
        item_loader.add_value("landlord_phone", "914 87 09 87") 
        item_loader.add_value("landlord_email", "info@presidence.es")       
        yield item_loader.load_item()

        
       

        
        
          

        

      
     