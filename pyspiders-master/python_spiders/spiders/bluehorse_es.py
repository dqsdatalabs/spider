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
    name = 'bluehorse_es'
    execution_type='testing'
    country='spain'
    locale='es'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.bluehorse.es/results/?id_tipo_operacion=2&type=1%2C18%2C21%2C22%2C24&dt=&dormitorios_min=&precio_max=",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.bluehorse.es/results/?id_tipo_operacion=2&type=2%2C3%2C16%2C20%2C6&dt=&dormitorios_min=&precio_max=",

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

       
        for item in response.xpath("//div[@class='swiper-slide']/a/@href").extract():
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
    
        house = "".join(response.xpath("//div[@class='detallesFicha']/ul/li[strong[.='Type: ']]/text()").extract())
        if "house" in  house:
            item_loader.add_value("property_type", "house")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//div[@class='headerLeft']/h1/text()")
        item_loader.add_value("external_source", "Bluehorse_PySpider_"+ self.country + "_" + self.locale)
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
        
        floor = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Planta')]/text()[normalize-space()]").extract_first()
        if floor :  
            item_loader.add_value("floor", floor.replace("º","").strip())

        rent = response.xpath("//p[@class='precio']//text()").extract_first()
        if rent :     
            rent= rent.split(":")[1].split("/")[0]      
            item_loader.add_value("rent_string", rent.strip())
        
        elevator = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Ascensor')]/text()[normalize-space()]").extract_first()
        if elevator :
            item_loader.add_value("elevator", True)

     
        terrace = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Terraza')]/text()[normalize-space()]").extract_first()
        if terrace :
            item_loader.add_value("terrace", True)
        
        balcony=response.xpath("//div[@id='descripcionFicha2']/p//text()[contains(.,'Balcón') or contains(.,'balcón')]").getall()
        if balcony:
            item_loader.add_value("balcony", True)
        elif terrace:
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//div[@class='detallesFicha']//li[contains(.,'Amueblado')]/text()[normalize-space()]").extract_first()  
        if furnished:
            item_loader.add_value("furnished", True)

        desc = "".join(response.xpath("//div[@id='descripcionFicha2']/p//text()").extract())
        if desc :
            item_loader.add_value("description", desc.strip())
        
        if "lavadora" in desc:
            item_loader.add_value("washing_machine", True)
        if "lavavajillas" in desc:
            item_loader.add_value("dishwasher", True)
        
        if ("no mascotas" in desc.lower()) or ("no se aceptan mascotas" in desc.lower()) or ("mascotas no se aceptana" in desc.lower()):
            item_loader.add_value("pets_allowed", False)
        
        deposit = "/".join(response.xpath("//div[@id='descripcionFicha2']/p//text()[contains(.,'fianza')]").extract())
        deposit2 = "/".join(response.xpath("//div[@id='descripcionFicha2']/p//text()[contains(.,'Fianza')]").extract())
        depos = False
        if deposit:
            try:
                depos = deposit.split("fianza")[0].split("/")[1].split("+")[-1]
            except:
                pass
            if depos:
                depos = depos.strip().split(" ")[0]
        elif deposit2:
            try:
                depos = deposit2.split("Fianza")[1].split("/")[0].split("+")[-1]
            except:
                pass
            if depos:
                depos = depos.strip().split(" ")[0]
                if "-" in depos:
                    depos = depos.split("-")[0]
        
        if depos and depos.isdigit():
            rent = rent.replace("€","").replace(".","").strip()
            item_loader.add_value("deposit", str(int(rent)*int(depos.strip())))
            
        address = ",".join(response.xpath("//div[@class='detallesFicha']//li[contains(.,'Población') or contains(.,'Provincia') or contains(.,'Zona') ]/text()[normalize-space()]").extract() )   
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
                   
        energy_label=response.xpath("//div[@class='detallesFicha']//li[contains(.,'energética') or contains(.,'energética energética (emisiones)')]/text()[normalize-space()][.!=' NO ']").extract_first()
        if energy_label :
            item_loader.add_value("energy_label", energy_label.strip())

        img=response.xpath("//div[@class='fotorama']/a/@href").extract() 
        if img:
            images=[]
            for x in img:
                images.append(x)
            if images:
                item_loader.add_value("images",  list(set(images)))
        
        bathroom="".join(response.xpath("//ul/li/strong[contains(.,'Baños')]/parent::li/text()").getall()).strip()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.split(" ")[0])
        
        item_loader.add_xpath("latitude","//div[@id='mapa']/@data-lat")
        item_loader.add_xpath("longitude","//div[@id='mapa']/@data-lng")
        item_loader.add_value("landlord_name", "bluehorse")
        item_loader.add_value("landlord_phone", "952638462") 
        item_loader.add_value("landlord_email", "info@bluehorse.es") 

        
        status = response.xpath("//div[@id='etiqueta']/span/text()").get()
        if status and "Disponible" in status:
            yield item_loader.load_item()
        elif not status:
            yield item_loader.load_item()
            

        
       

        
        
          

        

      
     